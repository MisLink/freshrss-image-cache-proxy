use std::{collections::HashMap, os::macos::raw::stat};

use sha2::{Digest, Sha256};
use tracing_subscriber::{
    fmt::{format::Pretty, time::UtcTime},
    prelude::*,
};
use tracing_web::{performance_layer, MakeWebConsoleWriter};
use worker::{
    console_debug, event, Context, Data, Env, Error, Fetch, Object, Request, Response,
    ResponseBody, Result, RouteContext, Router, Url,
};

fn get_r2_key(url: &str) -> String {
    let hash = Sha256::digest(url.as_bytes());
    let elen = base16ct::encoded_len(&hash);
    let mut dst = vec![0u8; elen];
    let hex = base16ct::lower::encode_str(&hash, &mut dst).expect("dst length is correct");
    format!("{}/{}/{}", &hex[0..2], &hex[2..4], &hex[4..])
}

async fn put_in_r2(ctx: &RouteContext<()>, url: &str, res: Response) -> Result<()> {
    let key = get_r2_key(url);
    let bucket = ctx.bucket("R2_BINDING")?;
    let r = bucket.head(&key).await?;
    if r.is_some() {
        console_debug!("object {} already exists in R2", &key);
        return Ok(());
    }
    let value = match res.body().clone() {
        ResponseBody::Empty => Data::Empty,
        ResponseBody::Body(items) => Data::Bytes(items),
        ResponseBody::Stream(readable_stream) => Data::ReadableStream(readable_stream),
    };
    let _ = bucket
        .put(&key, value)
        .custom_metadata(HashMap::from([("url".to_string(), url.to_string())]))
        .execute()
        .await?;
    Ok(())
}

async fn get_from_r2(ctx: &RouteContext<()>, url: &str) -> Result<Option<Object>> {
    let key = get_r2_key(url);
    let bucket = ctx.bucket("R2_BINDING")?;
    bucket.get(&key).execute().await
}

async fn cache_url(ctx: &RouteContext<()>, url_str: &str) -> Result<Response> {
    let url = Url::parse(url_str)?;
    let mut res = Fetch::Url(url).send().await?;
    match res.status_code() {
        200..300 => {
            put_in_r2(ctx, url_str, res.cloned()?).await?;
            Ok(res)
        }
        400.. => {
            if let Some(obj) = get_from_r2(ctx, url_str).await? {
                if let Some(body) = obj.body() {
                    tracing::info!(
                        url = url_str,
                        key = obj.key(),
                        "object found in R2, returning cached response",
                    );
                    return Response::from_body(body.response_body()?);
                }
            }
            tracing::warn!(
                url = url_str,
                status = res.status_code(),
                body = res.text().await.unwrap_or_default(),
                "object not found in R2, returning fallback response",
            );
            let fallback_url = ctx.env.var("FALLBACK_URL")?.to_string();
            let url = Url::parse(&fallback_url)?;
            Fetch::Url(url).send().await
        }
        _ => Err(Error::from("unexpected status code from origin")),
    }
}

#[tracing::instrument(err, skip(ctx))]
async fn get(req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let q = req
        .url()?
        .query_pairs()
        .find(|(k, _)| k == "url")
        .map(|(_, v)| v.into_owned());
    let url = q.ok_or_else(|| Error::from("missing url parameter"))?;
    cache_url(&ctx, &url).await
}

#[derive(serde::Deserialize)]
struct PostRequest {
    url: String,
    access_token: String,
}

#[tracing::instrument(err, skip(ctx))]
async fn post(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let req: PostRequest = req.json().await?;
    let api_token = ctx.env.var("API_TOKEN")?.to_string();
    if req.access_token != api_token {
        return Response::error("invalid access token", 403);
    }
    cache_url(&ctx, &req.url).await?;
    Response::empty()
}

#[event(start)]
fn start() {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_ansi(false)
        .with_timer(UtcTime::rfc_3339())
        .with_writer(MakeWebConsoleWriter::default());
    let perf_layer = performance_layer().with_details_from_fields(Pretty::default());
    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(perf_layer)
        .init();
}

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    Router::new()
        .get_async("/", get)
        .post_async("/", post)
        .run(req, env)
        .await
}

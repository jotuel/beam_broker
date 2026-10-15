import beam_broker/protocol
import beam_broker/web
import gleam/erlang/process
import gleam/http.{Get, Post}
import gleam/string
import simplifile
import wisp.{type Request, type Response}

pub type Context {
  Context(broker_ref: process.Name(protocol.Request))
}

pub fn handle_request(request: Request, ctx: Context) -> Response {
  use request <- web.middleware(request)

  case wisp.path_segments(request) {
    [] -> home_page(request)
    ["api", "topics"] -> todo
    ["api", "consumers"] -> todo
    ["api", "publishers"] -> todo
    ["api", "publish"] -> todo

    ["api", "topics", "list"] -> list_topics(request, ctx)
    ["api", "topics", "options"] -> todo
    ["api", "consumers", "options"] -> todo
    ["api", "consumer", "events"] -> todo
    _ -> wisp.not_found()
  }
}

fn home_page(request: Request) -> Response {
  use <- wisp.require_method(request, Get)
  case simplifile.read("public/index.html") {
    Ok(html) -> wisp.html_response(html, 200)
    Error(_) -> wisp.html_response("<h1>Index.html not found</h1>", 500)
  }
}

fn list_topics(request: Request, ctx: Context) -> Response {
  use <- wisp.require_method(request, Get)
  let subject = process.named_subject(ctx.broker_ref)
  let result = process.call(subject, 1000, protocol.ListTopics)
  wisp.html_response(string.join(result, "\n"), 200)
}

import beam_broker/protocol
import beam_broker/web
import gleam/erlang/process
import gleam/http.{Get, Post}
import wisp.{type Request, type Response}

pub fn handle_request(
  request: Request,
  _broker_ref: process.Name(protocol.Request),
) -> Response {
  use request <- web.middleware(request)

  case wisp.path_segments(request) {
    [] -> home_page(request)
    _ -> wisp.not_found()
  }
}

fn home_page(request: Request) -> Response {
  use <- wisp.require_method(request, Get)

  wisp.ok()
  |> wisp.html_body("Hello, world!")
}

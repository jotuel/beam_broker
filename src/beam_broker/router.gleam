import beam_broker/web
import wisp

pub fn handle_request(request: wisp.Request) -> wisp.Response {
  use request <- web.middleware(request)

  let body = "<h1>Hello, world!</h1>"

  wisp.html_response(body, 200)
}

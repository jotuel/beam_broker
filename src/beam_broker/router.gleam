import beam_broker/protocol.{type BrokerMessage, ClientRequest}
import beam_broker/web
import gleam/erlang/process
import gleam/http.{Get, Post}
import gleam/list
import gleam/string
import simplifile
import wisp.{type Request, type Response}

pub type Context {
  Context(broker_ref: process.Name(BrokerMessage))
}

pub fn handle_request(request: Request, ctx: Context) -> Response {
  use request <- web.middleware(request)

  case wisp.path_segments(request) {
    [] -> home_page(request)
    ["api", "topics"] -> create_topic(request, ctx)
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

fn create_topic(request: Request, ctx: Context) -> Response {
  use <- wisp.require_method(request, Post)
  use form_data <- wisp.require_form(request)
  case list.key_find(form_data.values, "topic_name") {
    Ok(topic_name) -> {
      let trimmed_name = string.trim(topic_name)
      case trimmed_name == "" {
        True -> wisp.html_response("Topic name cannot be empty", 400)
        False -> {
          let subject = process.named_subject(ctx.broker_ref)
          case
            process.call(subject, 1000, fn(reply_to) {
              ClientRequest(protocol.CreateTopic(reply_to, trimmed_name))
            })
          {
            Ok(_) ->
              wisp.html_response("Topic '" <> trimmed_name <> "' created!", 200)
            Error(error) ->
              case error {
                protocol.TopicAlreadyExists ->
                  wisp.html_response("Topic already exists", 400)
                protocol.TopicFailedToStart ->
                  wisp.html_response("Topic actor failed to start", 500)
              }
          }
        }
      }
    }
    Error(Nil) -> wisp.bad_request("no topic_name found")
  }
}

fn list_topics(request: Request, ctx: Context) -> Response {
  use <- wisp.require_method(request, Get)
  let subject = process.named_subject(ctx.broker_ref)
  let result =
    process.call(subject, 1000, fn(reply_to) {
      ClientRequest(protocol.ListTopics(reply_to))
    })
  wisp.html_response(string.join(result, "\n"), 200)
}

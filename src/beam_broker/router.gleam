import beam_broker/protocol.{type BrokerMessage, ClientRequest}
import beam_broker/web
import gleam/erlang/process
import gleam/http.{Get, Post}
import gleam/list
import gleam/result
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
    ["api", "consumers"] -> create_consumer(request, ctx)
    ["api", "publishers"] -> create_publisher(request, ctx)
    ["api", "publish"] -> publish_event(request, ctx)
    ["api", "topics", "list"] -> list_topics(request, ctx)
    ["api", "publishers", "options"] -> list_publishers(request, ctx)
    ["api", "consumers", "options"] -> list_consumers(request, ctx)
    ["api", "consumer", "events"] -> get_events(request, ctx)
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

fn create_consumer(request: Request, ctx: Context) -> Response {
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
              ClientRequest(protocol.CreateConsumer(reply_to, trimmed_name))
            })
          {
            Ok(_) ->
              wisp.html_response(
                "Consumer '" <> trimmed_name <> "' created!",
                200,
              )
            Error(error) ->
              case error {
                protocol.ConsumerAlreadyExists ->
                  wisp.html_response("Consumer already registered", 400)
                protocol.ConsumerTargetNotFound ->
                  wisp.html_response("Topic not found", 400)
                protocol.ConsumerFailedToStart ->
                  wisp.html_response("Consumer actor failed to start", 500)
              }
          }
        }
      }
    }
    Error(Nil) -> wisp.bad_request("no topic_name found")
  }
}

fn create_publisher(request: Request, ctx: Context) -> Response {
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
              ClientRequest(protocol.CreatePublisher(reply_to, trimmed_name))
            })
          {
            Ok(_) ->
              wisp.html_response(
                "Publisher '" <> trimmed_name <> "' created!",
                200,
              )
            Error(error) ->
              case error {
                protocol.PublisherAlreadyExists ->
                  wisp.html_response("Publisher already registered", 400)
                protocol.PublisherTargetNotFound ->
                  wisp.html_response("Topic not found", 400)
                protocol.PublisherFailedToStart ->
                  wisp.html_response("Publisher actor failed to start", 500)
              }
          }
        }
      }
    }
    Error(Nil) -> wisp.bad_request("no topic_name found")
  }
}

fn publish_event(request: Request, ctx: Context) -> Response {
  use <- wisp.require_method(request, Post)
  use form_data <- wisp.require_form(request)

  let publisher_result =
    list.key_find(form_data.values, "publisher_name")
    |> result.map(string.trim)

  let payload_result =
    list.key_find(form_data.values, "payload")
    |> result.map(string.trim)

  case publisher_result, payload_result {
    Ok(publisher), Ok(payload) if publisher != "" && payload != "" -> {
      let subject = process.named_subject(ctx.broker_ref)
      case
        process.call(subject, 1000, fn(reply_to) {
          ClientRequest(protocol.PublishEvent(reply_to, publisher, payload))
        })
      {
        Ok(_) ->
          wisp.html_response("Event published to '" <> publisher <> "'!", 200)
        Error(error) ->
          case error {
            protocol.WriteFailed ->
              wisp.html_response("Failed to write event", 500)
            protocol.PublisherNotFound ->
              wisp.html_response("Publisher not found", 400)
          }
      }
    }
    _, _ -> wisp.bad_request("Missing or empty publisher_name or payload")
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

fn list_publishers(request: Request, ctx: Context) -> Response {
  use <- wisp.require_method(request, Get)
  let subject = process.named_subject(ctx.broker_ref)
  let options_html =
    process.call(subject, 1000, fn(reply_to) {
      ClientRequest(protocol.ListPublishers(reply_to))
    })
    |> list.map(fn(publisher) {
      "<option value=\"" <> publisher <> "\">" <> publisher <> "</option>"
    })
    |> string.join("")
  case options_html == "" {
    True -> {
      let result =
        "<option value=\"\">-- Select a Publisher --</option>" <> options_html
      wisp.html_response(result, 200)
    }
    False ->
      wisp.html_response(
        "<option value=\"\">No publishers registered</option>",
        400,
      )
  }
}

fn list_consumers(request: Request, ctx: Context) -> Response {
  use <- wisp.require_method(request, Get)
  let subject = process.named_subject(ctx.broker_ref)
  let options_html =
    process.call(subject, 1000, fn(reply_to) {
      ClientRequest(protocol.ListConsumers(reply_to))
    })
    |> list.map(fn(consumer) {
      "<option value=\"" <> consumer <> "\">" <> consumer <> "</option>"
    })
    |> string.join("")
  case options_html == "" {
    True -> {
      let result =
        "<option value=\"\">-- Select a Consumer --</option>" <> options_html
      wisp.html_response(result, 200)
    }
    False ->
      wisp.html_response(
        "<option value=\"\">No consumers registered</option>",
        400,
      )
  }
}

fn get_events(request: Request, ctx: Context) -> Response {
  use <- wisp.require_method(request, Get)
  let query_params = wisp.get_query(request)

  let consumer_result =
    list.key_find(query_params, "consumer_name")
    |> result.map(string.trim)

  case consumer_result {
    Ok(consumer) if consumer != "" -> {
      let subject = process.named_subject(ctx.broker_ref)
      case
        process.call(subject, 1000, fn(reply_to) {
          ClientRequest(protocol.ReadEvents(reply_to, consumer))
        })
      {
        Ok(events) -> {
          let html_events =
            events
            |> list.map(fn(e) { "<div>" <> e <> "</div>" })
            |> string.join("")
          let final_html = case html_events {
            "" -> "<p>No events yet...</p>"
            _ -> html_events
          }
          wisp.html_response(final_html, 200)
        }
        Error(error) ->
          case error {
            protocol.ConsumerNotFound ->
              wisp.html_response("Consumer not found!", 400)
          }
      }
    }
    _ -> wisp.bad_request("Missing or empty consumer_name")
  }
}

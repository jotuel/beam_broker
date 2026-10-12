import beam_broker/protocol.{type PublisherMessage, type TopicMessage}
import gleam/erlang/process.{type Subject}
import gleam/otp/actor

pub type Args {
  Args(
    name: String,
    topic: Subject(TopicMessage),
    reply_to: Subject(protocol.Request),
  )
}

type State {
  State(topic: Subject(TopicMessage))
}

/// Start a publisher actor
///
/// Publisher is what pushes events to its topic
///
pub fn start_actor(args: Args) {
  let started =
    actor.new_with_initialiser(5000, fn(own) {
      Ok(
        actor.initialised(State(args.topic))
        |> actor.returning(own),
      )
    })
    |> actor.on_message(handle_message)
    |> actor.start
  case started {
    Ok(result) -> {
      process.send(
        args.reply_to,
        protocol.RegisterPublisher(args.name, result.data),
      )
    }
    Error(_) -> Nil
  }
  started
}

fn handle_message(state: State, message: PublisherMessage) {
  case message {
    protocol.Write(input) -> {
      process.send(state.topic, protocol.Append(input))
      actor.continue(state)
    }
  }
}

import beam_broker/protocol.{type PublisherMessage, type TopicMessage}
import gleam/erlang/process.{type Subject}
import gleam/otp/actor

pub type Args {
  Args(topic: Subject(TopicMessage))
}

type State {
  State(topic: Subject(TopicMessage))
}

/// Start a publisher actor
///
/// Publisher is what pushes events to its topic
///
pub fn start_actor(args: Args) {
  let state = State(args.topic)
  actor.new(state)
  |> actor.on_message(handle_message)
  |> actor.start
}

fn handle_message(state: State, message: PublisherMessage) {
  case message {
    protocol.Write(input) -> {
      process.send(state.topic, protocol.Append(input))
      actor.continue(state)
    }
  }
}

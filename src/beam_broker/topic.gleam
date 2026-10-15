import beam_broker/protocol.{type TopicMessage}
import gleam/erlang/process.{type Subject}
import gleam/otp/actor
import logging
import simplifile

pub type Args {
  Args(file_path: String, reply_to: Subject(protocol.BrokerMessage))
}

pub type State {
  State(file_path: String, own: Subject(TopicMessage), next_offset: Int)
}

/// Start a topic actor
///
/// Topic is an event log where other actors can publish to,
/// and consume from, events. A topic is a prerequisite for
/// both publishers and consumers
///
pub fn start_actor(
  args: Args,
) -> Result(actor.Started(Subject(TopicMessage)), actor.StartError) {
  let _ = simplifile.create_file(args.file_path)

  let started =
    actor.new_with_initialiser(5000, fn(own) {
      Ok(
        actor.initialised(State(
          file_path: args.file_path,
          own: own,
          next_offset: 0,
        ))
        |> actor.returning(own),
      )
    })
    |> actor.on_message(handle_message)
    |> actor.start
  case started {
    Ok(result) -> {
      process.send(
        args.reply_to,
        protocol.RegisterTopic(args.file_path, result.data),
      )
    }
    Error(_) -> Nil
  }
  started
}

fn handle_message(state: State, message: TopicMessage) {
  case message {
    protocol.Append(input) -> {
      case simplifile.append(state.file_path, input) {
        Ok(_) -> actor.continue(state)
        Error(error) -> {
          logging.log(logging.Warning, simplifile.describe_error(error))
          actor.continue(state)
        }
      }
    }

    protocol.Read(replyto) -> {
      case simplifile.read(state.file_path) {
        Ok(result) -> {
          actor.send(replyto, protocol.Receive(result))
          actor.continue(state)
        }
        Error(error) -> {
          logging.log(logging.Error, simplifile.describe_error(error))
          actor.continue(state)
        }
      }
    }
  }
}

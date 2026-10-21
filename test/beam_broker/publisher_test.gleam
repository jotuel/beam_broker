import beam_broker/protocol
import beam_broker/publisher
import beam_broker/topic
import gleam/erlang/process
import gleeunit
import simplifile

pub fn main() {
  gleeunit.main()
}

pub fn write_test() {
  let name = "write_test"

  let broker_subject = process.new_subject()
  let topic_subject = start_topic(name, broker_subject)
  let args =
    publisher.Args(name: name, topic: topic_subject, reply_to: broker_subject)
  let assert Ok(started) = publisher.start_actor(args)
  let publisher_subject = started.data

  // Publishing
  let payload = "Hello world!"
  case
    process.call(publisher_subject, 1000, fn(reply_to) {
      protocol.Write(reply_to, payload)
    })
  {
    Ok(_) -> Nil
    Error(_) -> panic as "failed to write"
  }
}

fn start_topic(
  file_path: String,
  broker: process.Subject(protocol.BrokerMessage),
) {
  let _ = simplifile.delete(file_path)
  let topic_args = topic.Args(file_path: file_path, reply_to: broker)
  let assert Ok(topic_started) = topic.start_actor(topic_args)
  let _ = process.receive(broker, 1000)
  topic_started.data
}

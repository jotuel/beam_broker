import beam_broker/consumer
import beam_broker/protocol
import beam_broker/topic
import gleam/erlang/process
import gleam/list
import gleam/string
import gleeunit
import simplifile

pub fn main() {
  gleeunit.main()
}

pub fn read_test() {
  let name = "read_test"

  let broker_subject = process.new_subject()
  let topic_subject = start_topic(name, broker_subject)
  let args =
    consumer.Args(name: name, reply_to: broker_subject, topic: topic_subject)
  let assert Ok(started) = consumer.start_actor(args)
  let consumer_subject = started.data

  let message = "Hello world!"
  let _ =
    process.call(topic_subject, 1000, fn(reply_to) {
      protocol.Append(reply_to, message)
    })

  // Reading
  let result =
    process.call(consumer_subject, 1000, fn(reply_to) {
      protocol.GetEvents(reply_to)
    })
  assert list.length(result) == 1
  let assert Ok(found) = list.first(result)
  assert string.contains(found, message)
  let _ = simplifile.delete(name)
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

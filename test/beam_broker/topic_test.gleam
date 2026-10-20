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

pub fn register_append_read_topic_test() {
  let test_file = "test.txt"
  let _ = simplifile.delete(test_file)

  let broker_subject = process.new_subject()
  let args = topic.Args(file_path: test_file, reply_to: broker_subject)
  let assert Ok(started) = topic.start_actor(args)
  let topic_subject = started.data

  let registration_msg = process.receive(broker_subject, 1000)

  // Self registration
  case registration_msg {
    Ok(protocol.RegisterTopic(path, _subject)) -> {
      assert path == test_file
    }
    _ -> panic as "self registration failed"
  }

  // Writing to topic
  assert process.call(topic_subject, 1000, fn(reply_to) {
      protocol.Append(reply_to, "Hello world!")
    })
    == Ok(Nil)

  // Reading from topic
  let read_response =
    process.call(topic_subject, 1000, fn(reply_to) {
      protocol.Read(reply_to, 0)
    })
  let assert Ok(protocol.ReadResult(events, new_index)) = read_response
  assert new_index == 1
  assert list.length(events) == 1

  let assert Ok(first_event) = list.first(events)
  assert string.contains(first_event, "Hello world!")

  let _ = simplifile.delete(test_file)
}

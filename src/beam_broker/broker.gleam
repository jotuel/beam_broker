import beam_broker/consumer
import beam_broker/protocol.{type BrokerMessage, type Response}
import beam_broker/publisher
import beam_broker/topic
import gleam/dict
import gleam/erlang/process.{type Subject}
import gleam/otp/actor
import gleam/otp/factory_supervisor as factory
import logging

pub type Args {
  Args(
    name: process.Name(BrokerMessage),
    topic: process.Name(
      factory.Message(topic.Args, Subject(protocol.TopicMessage)),
    ),
    consumer: process.Name(
      factory.Message(consumer.Args, Subject(protocol.ConsumerMessage)),
    ),
    publisher: process.Name(
      factory.Message(publisher.Args, Subject(protocol.PublisherMessage)),
    ),
  )
}

type State {
  State(
    topic: process.Name(
      factory.Message(topic.Args, Subject(protocol.TopicMessage)),
    ),
    consumer: process.Name(
      factory.Message(consumer.Args, Subject(protocol.ConsumerMessage)),
    ),
    publisher: process.Name(
      factory.Message(publisher.Args, Subject(protocol.PublisherMessage)),
    ),
    topics: dict.Dict(String, Subject(protocol.TopicMessage)),
    publishers: dict.Dict(String, Subject(protocol.PublisherMessage)),
  )
}

/// Start a broker actor
///
/// Broker is the middleware that passes instructions
/// from the API layer to the core program
pub fn start_actor(args: Args) {
  let state =
    State(
      topics: dict.new(),
      publishers: dict.new(),
      topic: args.topic,
      consumer: args.consumer,
      publisher: args.publisher,
    )
  actor.new(state)
  |> actor.named(args.name)
  |> actor.on_message(handle_message)
  |> actor.start
}

fn handle_message(state: State, message: BrokerMessage) {
  case message {
    protocol.Request(request, reply_to) -> {
      case request {
        protocol.CreateTopic(name) -> create_topic(name, state, reply_to)
        protocol.RegisterConsumer(topic) ->
          create_consumer(topic, state, reply_to)
        protocol.RegisterPublisher(topic) ->
          create_publisher(topic, state, reply_to)
        protocol.PublishEvent(publisher, event) ->
          publish(publisher, event, state, reply_to)
        protocol.ListTopics -> list_topics(state, reply_to)
      }
    }
  }
  actor.continue(state)
}

fn create_topic(
  name: String,
  state: State,
  reply_to: Subject(Response),
) -> State {
  case dict.get(state.topics, name) {
    Ok(_) -> {
      process.send(reply_to, protocol.TopicAlreadyExists)
      logging.log(logging.Warning, "topic already initialized: " <> name)
      state
    }
    Error(_) -> new_topic(name, state, reply_to)
  }
}

fn new_topic(name: String, state: State, reply_to: Subject(Response)) -> State {
  let supervisor = factory.get_by_name(state.topic)
  case factory.start_child(supervisor, topic.Args(file_path: name)) {
    Ok(started) -> {
      let subject = started.data
      let new_topics = dict.insert(state.topics, name, subject)
      process.send(reply_to, protocol.TopicCreated)
      State(..state, topics: new_topics)
    }
    Error(_) -> {
      logging.log(logging.Error, "failed to start topic actor: " <> name)
      state
    }
  }
}

fn create_consumer(topic: String, state: State, reply_to: Subject(Response)) {
  case dict.get(state.topics, topic) {
    Ok(found) -> new_consumer(found, topic, state, reply_to)
    Error(_) -> {
      process.send(reply_to, protocol.TopicNotFound)
      logging.log(logging.Warning, "topic not found: " <> topic)
      state
    }
  }
}

fn new_consumer(
  topic: Subject(protocol.TopicMessage),
  topic_name: String,
  state: State,
  reply_to: Subject(Response),
) -> State {
  let supervisor = factory.get_by_name(state.consumer)
  case factory.start_child(supervisor, consumer.Args(topic)) {
    Ok(started) -> {
      process.send(reply_to, protocol.ConsumerCreated)
      process.send(started.data, protocol.Poll)
      state
    }
    Error(_) -> {
      logging.log(
        logging.Error,
        "failed to start consumer actor for topic: " <> topic_name,
      )
      state
    }
  }
}

fn create_publisher(topic: String, state: State, reply_to: Subject(Response)) {
  case dict.get(state.topics, topic) {
    Ok(found) -> new_publisher(topic, found, state, reply_to)
    Error(_) -> {
      process.send(reply_to, protocol.TopicNotFound)
      logging.log(logging.Warning, "topic not found: " <> topic)
      state
    }
  }
}

fn new_publisher(
  name: String,
  topic: Subject(protocol.TopicMessage),
  state: State,
  reply_to: Subject(Response),
) -> State {
  let supervisor = factory.get_by_name(state.publisher)
  case factory.start_child(supervisor, publisher.Args(topic)) {
    Ok(started) -> {
      let subject = started.data
      let new_publishers = dict.insert(state.publishers, name, subject)
      process.send(reply_to, protocol.PublisherCreated)
      State(..state, publishers: new_publishers)
    }
    Error(_) -> {
      logging.log(
        logging.Error,
        "failed to initialize publisher actor for topic: " <> name,
      )
      state
    }
  }
}

fn publish(
  publisher: String,
  event: String,
  state: State,
  reply_to: Subject(Response),
) -> State {
  case dict.get(state.publishers, publisher) {
    Ok(found) -> {
      process.send(found, protocol.Write(event))
      process.send(reply_to, protocol.Published)
      state
    }
    Error(_) -> {
      process.send(reply_to, protocol.PublishFailed)
      logging.log(logging.Warning, "publisher not found: " <> publisher)
      state
    }
  }
}

fn list_topics(state: State, reply_to: Subject(Response)) {
  process.send(reply_to, protocol.Topics(dict.keys(state.topics)))
  state
}

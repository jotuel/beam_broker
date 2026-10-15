import gleam/erlang/process

pub type TopicMessage {
  Append(String)
  Read(replyto: process.Subject(ConsumerMessage))
}

pub type ConsumerMessage {
  Poll
  Receive(response: String)
}

pub type PublisherMessage {
  Write(event: String)
}

pub type BrokerMessage {
  ClientRequest(Request)
}

pub type Request {
  CreateTopic(name: String, reply_to: process.Subject(Response))
  RegisterTopic(name: String, subject: process.Subject(TopicMessage))

  CreateConsumer(topic: String, reply_to: process.Subject(Response))

  CreatePublisher(topic: String, reply_to: process.Subject(Response))
  RegisterPublisher(name: String, subject: process.Subject(PublisherMessage))

  PublishEvent(
    consumer: String,
    event: String,
    reply_to: process.Subject(Response),
  )
  ListTopics(reply_to: process.Subject(List(String)))
}

pub type Response {
  TopicCreated
  ConsumerCreated
  PublisherCreated
  Published

  TopicAlreadyExists
  TopicNotFound
  PublishFailed
}

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

pub type Request {
  CreateTopic(name: String)
  RegisterConsumer(topic: String)
  RegisterPublisher(topic: String)
  Publish(name: String, event: String)
  ListTopics
}

pub type Response {
  TopicCreated
  ConsumerCreated
  PublisherCreated
  Published

  Topics(List(String))

  TopicAlreadyExists
  TopicNotFound
  PublishFailed
}

pub type BrokerMessage {
  Request(request: Request, reply_to: process.Subject(Response))
}

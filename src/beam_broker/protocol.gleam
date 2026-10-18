import gleam/erlang/process

pub type TopicMessage {
  Append(reply_to: process.Subject(Result(Nil, Nil)), event: String)
  Read(replyto: process.Subject(ConsumerMessage))
}

pub type ConsumerMessage {
  Poll
  Receive(response: String)
  ResyncConsumer(topic: process.Subject(TopicMessage))
}

pub type PublisherMessage {
  Write(reply_to: process.Subject(Result(Nil, Nil)), event: String)
  ResyncPublisher(topic: process.Subject(TopicMessage))
}

pub type BrokerMessage {
  ClientRequest(Request)
  RegisterTopic(name: String, subject: process.Subject(TopicMessage))
  RegisterConsumer(name: String, subject: process.Subject(ConsumerMessage))
  RegisterPublisher(name: String, subject: process.Subject(PublisherMessage))
}

pub type Request {
  CreateTopic(
    reply_to: process.Subject(Result(Nil, CreateTopicError)),
    name: String,
  )

  CreateConsumer(
    reply_to: process.Subject(Result(Nil, CreateConsumerError)),
    topic: String,
  )

  CreatePublisher(
    reply_to: process.Subject(Result(Nil, CreatePublisherError)),
    topic: String,
  )

  PublishEvent(
    reply_to: process.Subject(Result(Nil, PublishError)),
    consumer: String,
    event: String,
  )
  ListTopics(reply_to: process.Subject(List(String)))
  ListPublishers(reply_to: process.Subject(List(String)))
}

pub type CreateTopicError {
  TopicAlreadyExists
  TopicFailedToStart
}

pub type CreateConsumerError {
  ConsumerAlreadyExists
  ConsumerTargetNotFound
  ConsumerFailedToStart
}

pub type CreatePublisherError {
  PublisherAlreadyExists
  PublisherTargetNotFound
  PublisherFailedToStart
}

pub type PublishError {
  PublisherNotFound
  WriteFailed
}

pub type Response {
  ConsumerCreated
  Published

  TopicNotFound
  PublishFailed
}

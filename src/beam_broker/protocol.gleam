import gleam/erlang/process

pub type TopicMessage {
  Append(String)
  Read(replyto: process.Subject(ConsumerMessage))
}

pub type ConsumerMessage {
  Poll
  Receive(response: String)
  ResyncConsumer(topic: process.Subject(TopicMessage))
}

pub type PublisherMessage {
  Write(event: String)
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

  CreatePublisher(topic: String, reply_to: process.Subject(Response))

  PublishEvent(
    consumer: String,
    event: String,
    reply_to: process.Subject(Response),
  )
  ListTopics(reply_to: process.Subject(List(String)))
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

pub type Response {
  ConsumerCreated
  Published

  TopicNotFound
  PublishFailed
}

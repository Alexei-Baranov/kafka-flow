namespace KafkaFlow.BatchConsume
{
    using System;
    using System.Collections.Generic;

    public class BatchConsumeMessageContext : IMessageContext
    {
        public BatchConsumeMessageContext(
            IConsumerContext consumer,
            IReadOnlyCollection<IMessageContext> batchMessage)
        {
            this.ConsumerContext = consumer;
            this.Message = new Message(null, batchMessage);
        }

        public Message Message { get; }

        public IMessageHeaders Headers { get; } = new MessageHeaders();

        public IConsumerContext ConsumerContext { get; }

        public IProducerContext ProducerContext => null;

        public IMessageContext SetMessage(object key, object value) =>
            throw new NotSupportedException($"{nameof(BatchConsumeMessageContext)} does not allow change the message");

        public IMessageContext TransformMessage(object message) => throw new NotImplementedException();
    }
}

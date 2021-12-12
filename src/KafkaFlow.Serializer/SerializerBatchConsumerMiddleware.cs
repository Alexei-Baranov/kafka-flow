namespace KafkaFlow
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using KafkaFlow.BatchConsume;

    /// <summary>
    /// Middleware to deserialize batch messages when consuming
    /// </summary>
    public class SerializerBatchConsumerMiddleware : IMessageMiddleware
    {
        private readonly ISerializer serializer;
        private readonly IMessageTypeResolver typeResolver;

        /// <summary>
        /// Initializes a new instance of the <see cref="SerializerBatchConsumerMiddleware"/> class.
        /// </summary>
        /// <param name="serializer">Instance of <see cref="ISerializer"/></param>
        /// <param name="typeResolver">Instance of <see cref="IMessageTypeResolver"/></param>
        public SerializerBatchConsumerMiddleware(
            ISerializer serializer,
            IMessageTypeResolver typeResolver)
        {
            this.serializer = serializer;
            this.typeResolver = typeResolver;
        }

        /// <summary>
        /// Deserializes the message using the passed serialized
        /// </summary>
        /// <param name="context">The <see cref="IMessageContext"/> containing the message and metadata</param>
        /// <param name="next">A delegate to the next middleware</param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException">Throw if message is not byte[]</exception>
        public async Task Invoke(IMessageContext context, MiddlewareDelegate next)
        {
            var messageType = this.typeResolver.OnConsume(context);

            if (messageType is null)
            {
                throw new ArgumentException("messageType is null");
            }

            if (context.Message.Value is null)
            {
                await next(context).ConfigureAwait(false);
                return;
            }

            var messageContexts = context.Message.Value as List<IMessageContext>;

            if (messageContexts.Any(messageContext => messageContext.Message.Value == null))
            {
                throw new InvalidOperationException(
                    $"{nameof(context.Message)} must be a byte array to be deserialized and it is '{context.Message.GetType().FullName}'");
            }

            List<IMessageContext> contexts = new List<IMessageContext>(); 

            foreach (var messageContext in messageContexts)
            {
                using (var stream = new MemoryStream(messageContext.Message.Value as byte[] ?? Array.Empty<byte>()))
                {
                    var data = await this.serializer
                        .DeserializeAsync(
                            stream,
                            messageType,
                            new SerializerContext(messageContext.ConsumerContext.Topic))
                        .ConfigureAwait(false);
                    contexts.Add(messageContext.SetMessage(messageContext.Message.Key, data));
                }
            }

            var batchContext = new BatchConsumeMessageContext(context.ConsumerContext, contexts);

            await next(batchContext).ConfigureAwait(false);
        }
    }
}

<?php

namespace Robertbaelde\EventSauceDynamoDb;

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Marshaler;
use Aws\ResultPaginator;
use EventSauce\EventSourcing\AggregateRootId;
use EventSauce\EventSourcing\Header;
use EventSauce\EventSourcing\Message;
use EventSauce\EventSourcing\MessageRepository;
use EventSauce\EventSourcing\Serialization\MessageSerializer;
use EventSauce\EventSourcing\UnableToPersistMessages;
use EventSauce\EventSourcing\UnableToRetrieveMessages;
use Generator;

class DynamoDbMessageRepository implements MessageRepository
{
    private string $tableName;
    private MessageSerializer $serializer;
    private array $config;

    public function __construct(array $config, string $tableName, MessageSerializer $serializer)
    {
        $this->config = $config;
        $this->tableName = $tableName;
        $this->serializer = $serializer;
    }

    /**
     * @throws UnableToPersistMessages
     */
    public function persist(Message ...$messages): void
    {

        if (count($messages) === 0) {
            return;
        }

        $marshaler = new Marshaler();
        $transactItems = [];

        /** @var Message $message */
        foreach ($messages as $message) {
            $payload = $this->serializer->serializeMessage($message);
            $transactItems[] = [
                'Put' => [
                    'TableName' => $this->tableName,
                    'Item' => $marshaler->marshalJson(json_encode([
                        'eventStream' => $message->aggregateRootId()->toString(),
                        'version' => $message->aggregateVersion(),
                        'payload' => json_encode($payload),
                    ])),
                    'ConditionExpression' => 'attribute_not_exists(version)',
                ]
            ];
        }

        $this->getClient()->transactWriteItems([
            'TransactItems' => $transactItems
        ]);

    }

    /**
     * @return Generator<Message>
     *
     * @throws UnableToRetrieveMessages
     */
    public function retrieveAll(AggregateRootId $id): Generator
    {
        $query = [
            'TableName' => $this->tableName,
            "ScanIndexForward" =>  true,
            'KeyConditionExpression' => 'eventStream = :v1',
            'ExpressionAttributeValues' => [
                ':v1' => ['S' => $id->toString()],
            ]
        ];

        $result = $this->getClient()->getPaginator('Query', $query);

        return $this->yieldMessagesForResult($result);
    }

    /**
     * @return Generator<Message>
     *
     * @throws UnableToRetrieveMessages
     */
    public function retrieveAllAfterVersion(AggregateRootId $id, int $aggregateRootVersion): Generator
    {
        $query = [
            'TableName' => $this->tableName,
            "ScanIndexForward" =>  true,
            'KeyConditionExpression' => 'eventStream = :v1 And version > :version',
            'ExpressionAttributeValues' => [
                ':v1' => ['S' => $id->toString()],
                ':version' => ['N' => (string) $aggregateRootVersion],
            ]
        ];

        $result = $this->getClient()->getPaginator('Query', $query);

        return $this->yieldMessagesForResult($result);
    }

    public function setTable(string $tableName)
    {
        $this->tableName = $tableName;
    }

    public function setAwsConfig(array $config)
    {
        $this->config = $config;
    }

    private function getClient(): DynamoDbClient
    {
        $sdk = new \Aws\Sdk($this->config);
        return $sdk->createDynamoDb();
    }

    private function yieldMessagesForResult(ResultPaginator $result)
    {
        $marshaler = new Marshaler();
        $items = $result->search('Items');

        $version = 0;

        foreach ($items as $item) {
            $payloadItem = $marshaler->unmarshalItem($item);
            /** @var Message $message */
            $message = $this->serializer->unserializePayload(json_decode($payloadItem['payload'], true));
            $version = max($version, $message->aggregateVersion());
            yield $message;
        }

        return $version;
    }
}

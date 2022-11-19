<?php

namespace Robertbaelde\EventSauceDynamoDb;

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Marshaler;
use Aws\ResultPaginator;
use EventSauce\EventSourcing\AggregateRootId;
use EventSauce\EventSourcing\Header;
use EventSauce\EventSourcing\Message;
use EventSauce\EventSourcing\MessageRepository;
use EventSauce\EventSourcing\OffsetCursor;
use EventSauce\EventSourcing\PaginationCursor;
use EventSauce\EventSourcing\Serialization\MessageSerializer;
use EventSauce\EventSourcing\UnableToPersistMessages;
use EventSauce\EventSourcing\UnableToRetrieveMessages;
use Generator;
use LogicException;
use Ramsey\Uuid\Uuid;
use Throwable;

class DynamoDbMessageRepository implements MessageRepository
{
    private string $tableName;
    private MessageSerializer $serializer;
    private array $config;

    public function __construct(private DynamoDbClient $client, string $tableName, MessageSerializer $serializer)
    {
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
            // Set event ID when event id does not exist
            $payload['headers'][Header::EVENT_ID] = $payload['headers'][Header::EVENT_ID] ?? Uuid::uuid4()->toString();
            $transactItems[] = [
                'Put' => [
                    'TableName' => $this->tableName,
                    'Item' => $marshaler->marshalJson(json_encode([
                        'eventStream' => $message->aggregateRootId()->toString(),
                        'version' => $message->aggregateVersion(),
                        'allEvents' => 'allEvents',
                        'timestamp' => $message->timeOfRecording()->getTimestamp(),
                        'payload' => json_encode($payload),
                    ])),
                    'ConditionExpression' => 'attribute_not_exists(version)',
                ]
            ];
        }

        $this->client->transactWriteItems([
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

        $result = $this->client->getPaginator('Query', $query);

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

        $result = $this->client->getPaginator('Query', $query);

        return $this->yieldMessagesForResult($result);
    }

    private function yieldMessagesForResult(ResultPaginator $result): Generator
    {
        $marshaler = new Marshaler();
        $items = $result->search('Items');

        foreach ($items as $item) {
            $payloadItem = $marshaler->unmarshalItem($item);
            /** @var Message $message */
            yield $message = $this->serializer->unserializePayload(json_decode($payloadItem['payload'], true));
        }

        return isset($message) ? $message->header(Header::AGGREGATE_ROOT_VERSION) ?: 0 : 0;
    }

    public function paginate(PaginationCursor $cursor): Generator
    {
        if ( ! $cursor instanceof TimestampCursor) {
            throw new LogicException(sprintf('Wrong cursor type used, expected %s, received %s', TimestampCursor::class, get_class($cursor)));
        }

        $result = $this->client->getPaginator('Query', [
            'TableName' => $this->tableName,
            'IndexName' => 'allEvents',
            'KeyConditionExpression' => 'allEvents = :v1 And #timestamp BETWEEN :beginTimestampIncluding AND :endTimestampIncluding',
            'ExpressionAttributeNames' => [
                '#timestamp' => 'timestamp',
            ],
            'ExpressionAttributeValues' => [
                ':v1' => ['S' => 'allEvents'],
                ':beginTimestampIncluding' => ['N' => $cursor->beginTimestampIncluding()],
                ':endTimestampIncluding' => ['N' => $cursor->endTimestampIncluding()],
            ],
        ]);

        $marshaler = new Marshaler();
        $items = $result->search('Items');

        foreach ($items as $item) {
            $payloadItem = $marshaler->unmarshalItem($item);
            yield $this->serializer->unserializePayload(json_decode($payloadItem['payload'], true));
        }

        return $cursor->nextPage();
    }
}

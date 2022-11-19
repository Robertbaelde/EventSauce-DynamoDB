<?php

namespace Robertbaelde\EventSauceDynamoDb;

use Aws\DynamoDb\DynamoDbClient;
use EventSauce\EventSourcing\AggregateRootId;
use EventSauce\EventSourcing\DefaultHeadersDecorator;
use EventSauce\EventSourcing\DotSeparatedSnakeCaseInflector;
use EventSauce\EventSourcing\Header;
use EventSauce\EventSourcing\Message;
use EventSauce\EventSourcing\MessageRepository;
use EventSauce\EventSourcing\OffsetCursor;
use EventSauce\EventSourcing\Serialization\ConstructingMessageSerializer;
use EventSauce\EventSourcing\UnableToPersistMessages;
use EventSauce\EventSourcing\UnableToRetrieveMessages;
use EventSauce\MessageRepository\TestTooling\MessageRepositoryTestCase;
use Ramsey\Uuid\Uuid;

class DynamoDbMessageRepositoryIntegrationTest extends MessageRepositoryTestCase
{
    private int $version = 1;

    public function setUp(): void
    {
        parent::setUp();
        $this->version = 1;

        $client = $this->getDynamodbClient();
        try {
            $r = $client->createTable([
                'AttributeDefinitions' => [
                    ['AttributeName' => 'eventStream', 'AttributeType' => 'S'],
                    ['AttributeName' => 'version', 'AttributeType' => 'N'],
                    ['AttributeName' => 'allEvents', 'AttributeType' => 'S'],
                    ['AttributeName' => 'timestamp', 'AttributeType' => 'N'],
                ],
                'KeySchema' => [
                    ['AttributeName' => 'eventStream', 'KeyType' => 'HASH'],
                    ['AttributeName' => 'version', 'KeyType' => 'RANGE']
                ],
                'TableName' => 'test_events',
                'GlobalSecondaryIndexes' => [
                    [
                        'IndexName' => 'allEvents',
                        'KeySchema' => [
                            [
                                'AttributeName' => 'allEvents',
                                'KeyType' => 'HASH'
                            ],
                            [
                                'AttributeName' => 'timestamp',
                                'KeyType' => 'RANGE'
                            ]
                        ],
                        'Projection' => [
                            'ProjectionType' => 'ALL'
                        ],
                        'ProvisionedThroughput' => [
                            'ReadCapacityUnits' => 10,
                            'WriteCapacityUnits' => 10
                        ]
                    ]
                ],
                'ProvisionedThroughput' => [
                    'ReadCapacityUnits' => 10,
                    'WriteCapacityUnits' => 10
                ]
            ]);
        } catch (\Exception $e){
        }

        $scanResult = $client->scan([
            'TableName' => 'test_events',
        ]);
        foreach($scanResult->toArray()['Items'] as $item){
            $key = [
                'eventStream' => $item['eventStream'],
            ];

            if(array_key_exists('version', $item)){
                $key['version'] = $item['version'];
            }
            $client->deleteItem([
                'TableName' => 'test_events',
                'Key' => $key
            ]);
        }
    }

    protected function messageRepository(): MessageRepository
    {
        return new DynamoDbMessageRepository($this->getDynamodbClient(), 'test_events', new ConstructingMessageSerializer());
    }

    private function getDynamodbClient(): DynamoDbClient
    {
        $sdk = new \Aws\Sdk([
            'endpoint'   => 'http://localhost:8000',
            'region'   => 'us-east-1',
            'version'  => 'latest',
            'credentials' => [
                'key' => 'AWS_KEY',
                'secret' => 'AWS_SECRET'
            ]
        ]);
        return $sdk->createDynamoDb();
    }

    protected function aggregateRootId(): AggregateRootId
    {
        return DummyAggregateRootId::generate();
    }

    protected function eventId(): string
    {
        return Uuid::uuid4()->toString();
    }

    /**
     * @test
     */
    public function fetching_the_first_page_for_pagination(): void
    {
        $repository = $this->messageRepository();
        $messages = [];

        for ($i = 0; $i < 10; $i++) {
            $messages[] = $this->createMessage('number: ' . $i)
                ->withTimeOfRecording(new \DateTimeImmutable('2020-01-01 00:00:0' . $i))
                ->withHeader(Header::AGGREGATE_ROOT_VERSION, $i)
                ->withHeader(Header::EVENT_ID, Uuid::uuid4()->toString());
        }

        $repository->persist(...$messages);

        // returns messages from 00:00:00 until 00:00:04
        $page = $repository->paginate(TimestampCursor::fromStart(3, (new \DateTimeImmutable('2020-01-01 00:00:00'))->getTimestamp()));
        $messagesFromPage = iterator_to_array($page, false);

        $expectedMessages = array_slice($messages, 0, 4);
        $cursor = $page->getReturn();

        self::assertEquals($expectedMessages, $messagesFromPage);
        self::assertInstanceOf(TimestampCursor::class, $cursor);
    }

    /**
     * @test
     */
    public function fetching_the_next_page_for_pagination(): void
    {
        $repository = $this->messageRepository();
        $messages = [];

        for ($i = 0; $i < 10; $i++) {
            $messages[] = $this->createMessage('number: ' . $i)
                ->withTimeOfRecording(new \DateTimeImmutable('2020-01-01 00:00:0' . $i))
                ->withHeader(Header::AGGREGATE_ROOT_VERSION, $i)
                ->withHeader(Header::EVENT_ID, Uuid::uuid4()->toString());
        }

        $repository->persist(...$messages);
        // returns messages from 00:00:00 until 00:00:04
        $page = $repository->paginate(TimestampCursor::fromStart(3, (new \DateTimeImmutable('2020-01-01 00:00:00'))->getTimestamp()));
        iterator_to_array($page, false);
        $cursor = $page->getReturn();

        $page = $repository->paginate($cursor);
        $messagesFromPage = iterator_to_array($page, false);
        $expectedMessages = array_slice($messages, 4, 4);
        self::assertEquals($expectedMessages, $messagesFromPage);

        $page = $repository->paginate($page->getReturn());
        $messagesFromPage = iterator_to_array($page, false);
        $expectedMessages = array_slice($messages, 8, 2);
        self::assertEquals($expectedMessages, $messagesFromPage);

        self::assertEquals($expectedMessages, $messagesFromPage);
        self::assertInstanceOf(TimestampCursor::class, $cursor);
    }

    /**
     * @test
     */
    public function failing_to_persist_messages(): void
    {
        $this->markTestSkipped('todo');
        $this->tableName = 'invalid';
        $repository = $this->messageRepository();

        self::expectException(UnableToPersistMessages::class);

        $message = $this->createMessage('one');
        $repository->persist($message);
    }

    /**
     * @test
     */
    public function failing_to_retrieve_all_messages(): void
    {
        $this->markTestSkipped('todo');
        $this->tableName = 'invalid';
        $repository = $this->messageRepository();

        self::expectException(UnableToRetrieveMessages::class);

        $repository->retrieveAll($this->aggregateRootId);
    }

    /**
     * @test
     */
    public function failing_to_retrieve_messages_after_version(): void
    {
        $this->markTestSkipped('todo');
        $this->tableName = 'invalid';
        $repository = $this->messageRepository();

        self::expectException(UnableToRetrieveMessages::class);

        $repository->retrieveAllAfterVersion($this->aggregateRootId, 5);
    }

    /**
     * @test
     */
    public function failing_to_paginate(): void
    {
        $this->markTestSkipped('todo');
        if ( ! class_exists(OffsetCursor::class, true)) {
            self::markTestSkipped('Only run on 3.0 and up');
        }

        $this->tableName = 'invalid';
        $repository = $this->messageRepository();

        self::expectException(UnableToRetrieveMessages::class);

        iterator_to_array($repository->paginate(OffsetCursor::fromStart(limit: 10)));
    }

    protected function createMessage(string $value, AggregateRootId $id = null): Message
    {
        $id ??= $this->aggregateRootId;
        $type = (new DotSeparatedSnakeCaseInflector())->classNameToType(get_class($id));

        return (new DefaultHeadersDecorator())
            ->decorate(new Message(new DummyEvent($value)))
            ->withHeader(Header::AGGREGATE_ROOT_ID, $id)
            ->withHeader(Header::AGGREGATE_ROOT_ID_TYPE, $type)
            ->withHeader(Header::AGGREGATE_ROOT_VERSION, $this->version ++)
            ->withHeader(Header::EVENT_ID, Uuid::uuid4()->toString());
    }
}

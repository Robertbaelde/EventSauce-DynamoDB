<?php

namespace Robertbaelde\EventSauceDynamoDb;

use EventSauce\EventSourcing\AggregateRootId;
use EventSauce\EventSourcing\Header;
use EventSauce\EventSourcing\Message;
use EventSauce\EventSourcing\Serialization\ConstructingMessageSerializer;
use EventSauce\EventSourcing\Serialization\SerializablePayload;
use PHPUnit\Framework\TestCase;

class DynamoDbMessageRepositoryIntegrationTest extends TestCase
{
    private AggregateRootId $aggregateRootId;

    public function setUp(): void
    {
        parent::setUp();

        $sdk = new \Aws\Sdk($this->config());
        $client = $sdk->createDynamoDb();
        try {
            $r = $client->createTable([
                'AttributeDefinitions' => [
                    ['AttributeName' => 'eventStream', 'AttributeType' => 'S'],
                    ['AttributeName' => 'version', 'AttributeType' => 'N'],
                ],
                'KeySchema' => [
                    ['AttributeName' => 'eventStream', 'KeyType' => 'HASH'],
                    ['AttributeName' => 'version', 'KeyType' => 'RANGE']
                ],
                'TableName' => 'test_events',
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

        $this->aggregateRootId = DummyAggregateRootId::fromString('foo');
    }

    /** @test */
    public function it_can_persist_one_message()
    {
        $message = new Message(new DummyEvent(4), [
            Header::AGGREGATE_ROOT_ID => $this->aggregateRootId,
            Header::AGGREGATE_ROOT_VERSION => 1
        ]);

        $repo = $this->getRepository();
        $repo->persist(
            $message
        );

        $messages = $repo->retrieveAll($this->aggregateRootId);
        $messages = iterator_to_array($messages);
        $this->assertCount(1, $messages);

        $this->assertEquals($message->event(), $messages[0]->event());
    }

    /** @test */
    public function it_cant_save_message_with_same_version_twice()
    {
        $message = new Message(new DummyEvent(4), [
            Header::AGGREGATE_ROOT_ID => $this->aggregateRootId,
            Header::AGGREGATE_ROOT_VERSION => 1
        ]);

        $repo = $this->getRepository();
        $repo->persist(
            $message
        );

        $assertionThrown = false;

        try {
            $repo->persist(
                $message
            );
        } catch (\Exception $e){
            $assertionThrown = true;
        }

        $this->assertTrue($assertionThrown);

        $messages = $repo->retrieveAll($this->aggregateRootId);
        $messages = iterator_to_array($messages);
        $this->assertCount(1, $messages);

        $this->assertEquals($message->event(), $messages[0]->event());
    }

    /** @test */
    public function it_can_perisist_same_version_for_different_aggregates()
    {
        $message = new Message(new DummyEvent(4), [
            Header::AGGREGATE_ROOT_ID => $this->aggregateRootId,
            Header::AGGREGATE_ROOT_VERSION => 1
        ]);

        $repo = $this->getRepository();
        $repo->persist(
            $message
        );

        $message = new Message(new DummyEvent(4), [
            Header::AGGREGATE_ROOT_ID => DummyAggregateRootId::fromString('bar'),
            Header::AGGREGATE_ROOT_VERSION => 1
        ]);

        $repo->persist(
            $message
        );

        $messages = $repo->retrieveAll($this->aggregateRootId);
        $messages = iterator_to_array($messages);
        $this->assertCount(1, $messages);

        $this->assertEquals($message->event(), $messages[0]->event());

        $messages = $repo->retrieveAll(DummyAggregateRootId::fromString('bar'));
        $messages = iterator_to_array($messages);
        $this->assertCount(1, $messages);

        $this->assertEquals($message->event(), $messages[0]->event());
    }

    /** @test */
    public function when_one_version_overlaps_it_doesnt_persist_any_of_the_messages()
    {
        $message = new Message(new DummyEvent(4), [
            Header::AGGREGATE_ROOT_ID => $this->aggregateRootId,
            Header::AGGREGATE_ROOT_VERSION => 1
        ]);

        $repo = $this->getRepository();
        $repo->persist(
            $message
        );

        $exceptionThrown = false;

        try {
            $repo->persist(
                new Message(new DummyEvent(2), [
                    Header::AGGREGATE_ROOT_ID => $this->aggregateRootId,
                    Header::AGGREGATE_ROOT_VERSION => 1
                ]),
                new Message(new DummyEvent(4), [
                    Header::AGGREGATE_ROOT_ID => $this->aggregateRootId,
                    Header::AGGREGATE_ROOT_VERSION => 2
                ])
            );
        } catch (\Exception $e){
            $exceptionThrown = true;
        }

        $this->assertTrue($exceptionThrown);

        $messages = $repo->retrieveAll($this->aggregateRootId);
        $messages = iterator_to_array($messages);
        $this->assertCount(1, $messages);

        $this->assertEquals($message->event(), $messages[0]->event());
    }

    /** @test */
    public function it_can_retrieve_after_specific_version()
    {
        $repo = $this->getRepository();
        $repo->persist(
            new Message(new DummyEvent(4), [
                Header::AGGREGATE_ROOT_ID => $this->aggregateRootId,
                Header::AGGREGATE_ROOT_VERSION => 1
            ]),
            new Message(new DummyEvent(4), [
                Header::AGGREGATE_ROOT_ID => $this->aggregateRootId,
                Header::AGGREGATE_ROOT_VERSION => 2
            ]),
            new Message(new DummyEvent(4), [
                Header::AGGREGATE_ROOT_ID => $this->aggregateRootId,
                Header::AGGREGATE_ROOT_VERSION => 3
            ])
        );

        $messages = $repo->retrieveAllAfterVersion($this->aggregateRootId, 1);
        $messages = iterator_to_array($messages);
        $this->assertCount(2, $messages);

        $this->assertEquals(2, $messages[0]->aggregateVersion());
        $this->assertEquals(3, $messages[1]->aggregateVersion());
    }

    private function getRepository(): DynamoDbMessageRepository
    {
        return new DynamoDbMessageRepository(
            $this->config(),
            'test_events',
            new ConstructingMessageSerializer()
        );
    }

    private function config(): array
    {
        return [
            'endpoint'   => 'http://localhost:8000',
            'region'   => 'us-east-1',
            'version'  => 'latest',
            'credentials' => [
                'key' => 'AWS_KEY',
                'secret' => 'AWS_SECRET'
            ]
        ];
    }
}

class DummyEvent implements SerializablePayload
{
    public function __construct(public $foo = 1)
    {
    }

    public function toPayload(): array
    {
        return [
            'foo' => $this->foo,
        ];
    }

    public static function fromPayload(array $payload): SerializablePayload
    {
        return new self($payload['foo']);
    }
}

class DummyAggregateRootId implements AggregateRootId
{
    public function __construct(protected string $aggregateRootId)
    {
    }

    public function toString(): string
    {
        return $this->aggregateRootId;
    }

    public static function fromString(string $aggregateRootId): AggregateRootId
    {
        return new self($aggregateRootId);
    }
}

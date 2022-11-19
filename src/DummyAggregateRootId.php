<?php

namespace Robertbaelde\EventSauceDynamoDb;

use EventSauce\EventSourcing\AggregateRootId;
use Ramsey\Uuid\Uuid;

class DummyAggregateRootId implements AggregateRootId
{
    public function __construct(private string $id)
    {
    }

    public function toString(): string
    {
        return $this->id;
    }

    public static function fromString(string $aggregateRootId): static
    {
        return new static($aggregateRootId);
    }

    public static function generate(): static
    {
        return new static(Uuid::uuid4()->toString());
    }
}

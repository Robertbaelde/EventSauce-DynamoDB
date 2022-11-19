<?php

namespace Robertbaelde\EventSauceDynamoDb;

use EventSauce\EventSourcing\Serialization\SerializablePayload;

class DummyEvent implements SerializablePayload
{

    public function __construct(public readonly string $value)
    {
    }

    public function toPayload(): array
    {
        return ['value' => $this->value];
    }

    public static function fromPayload(array $payload): static
    {
        return new static($payload['value']);
    }
}

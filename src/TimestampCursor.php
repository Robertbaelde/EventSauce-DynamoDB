<?php

namespace Robertbaelde\EventSauceDynamoDb;

use EventSauce\EventSourcing\PaginationCursor;

final class TimestampCursor implements PaginationCursor
{
    const SEPARATOR = '|';

    public function __construct(private int $secondsPerPage, private int $startTimestamp, private bool $isAtStart = false)
    {
    }

    public static function fromStart(int $secondsPerPage, int $startTimestamp): self
    {
        return new self($secondsPerPage, $startTimestamp, true);
    }

    public function toString(): string
    {
        return $this->startTimestamp . self::SEPARATOR . $this->secondsPerPage . self::SEPARATOR . $this->isAtStart;
    }

    public static function fromString(string $cursor): static
    {
        return new self(...explode(self::SEPARATOR, $cursor));
    }

    public function isAtStart(): bool
    {
        return $this->isAtStart;
    }

    public function beginTimestampIncluding(): int
    {
        return $this->startTimestamp;
    }

    public function endTimestampIncluding(): int
    {
        return $this->startTimestamp + $this->secondsPerPage;
    }

    public function nextPage(): self
    {
        return new self($this->secondsPerPage, $this->endTimestampIncluding() + 1, false);
    }
}

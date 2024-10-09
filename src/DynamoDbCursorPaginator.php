<?php

namespace Baopham\DynamoDb;

use BaoPham\DynamoDb\Facades\DynamoDb;
use Illuminate\Pagination\Cursor;

class DynamoDbCursorPaginator extends \Illuminate\Pagination\CursorPaginator
{
  public function __construct($items, $perPage, $cursor = null, array $options = [], private $lastEvaluatedKey = null)
  {
    parent::__construct($items, $perPage, $cursor, $options);

    // The base paginator sets the `hasMore` flag based on the existence of elements beyond the perPage-limit.
    // We can instead just lean on DynamoDB to tell us, based on the returned lastEvaluatedKey.
    $this->hasMore = (boolean) $lastEvaluatedKey;
  }

  /**
   * Return a cursor to the previous page.
   *
   * Since DynamoDB cannot page back we just always return null.
   *
   * @return null
   */
  public function previousCursor()
  {
    return null;
  }

  /**
   * Return a cursor to the next page.
   *
   * This is largely cloning the base method but then just returning a cursor holding the `lastEvaluatedKey` instead.
   *
   * @return Cursor|null
   */
  public function nextCursor()
  {
    if ((is_null($this->cursor) && ! $this->hasMore) ||
      (! is_null($this->cursor) && $this->cursor->pointsToNextItems() && ! $this->hasMore)) {
      return null;
    }

    if ($this->items->isEmpty()) {
      return null;
    }

    // The Cursor implementation expects the parameters to only be 1 level deep, so we JSON-Encode the lastEvaluatedKey,
    //  since it can hold both a PK and an SK.
    return new Cursor(['lastEvaluatedKey' => json_encode(DynamoDb::unmarshalItem($this->lastEvaluatedKey))]);
  }

  /**
   * Get the instance as an array.
   *
   * We patch in the first_page_url here, since we can always just jump back to the first page by not passing a start
   * key to the query.
   *
   * @return array|string[]
   */
  public function toArray()
  {
    return array_merge(parent::toArray(), [
      'first_page_url' => $this->url(null),
    ]);
  }
}

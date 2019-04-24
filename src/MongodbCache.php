<?php

namespace SimpleCache;

use MongoDB\Driver\Manager;
use MongoDB\Driver\Server;
use MongoDB\Driver\Query;
use MongoDB\Driver\BulkWrite;
use MongoDB\BSON\UTCDateTime;
use MongoDB\BSON\Serializable;

use JsonSerializable;

class MongodbCache implements \Psr\SimpleCache\CacheInterface
{
    protected $manager;
    protected $namespace;

    protected $assoc;

    /**
     * @param Manager|Server $manager
     * @param string $namespace
     */
    public function __construct($manager, $namespace){
        $this->manager = $manager;
        $this->namespace = $namespace;
    }

    /**
     * Fetches a value from the cache.
     *
     * @param string $key     The unique key of this item in the cache.
     * @param mixed  $default Default value to return if the key does not exist.
     *
     * @return mixed The value of the item from the cache, or $default in case of cache miss.
     *
     * @throws \Psr\SimpleCache\InvalidArgumentException
     *   MUST be thrown if the $key string is not a legal value.
     */
    public function get($key, $default = null){
        $this->validateKeyValue($key);

        $query = new Query(['_id' => (string) $key], ['limit' => 1, 'projection' => ['data' => 1, '_id'=>0]]);
        $cursor = $this->manager->executeQuery($this->namespace, $query);

        $result = current($cursor->toArray());

        if ($result === false || !isset($result->data))
            return $default;

        return $result->data;
    }

    /**
     * Persists data in the cache, uniquely referenced by a key with an optional expiration TTL time.
     *
     * @param string                 $key   The key of the item to store.
     * @param mixed                  $value The value of the item to store, must be serializable.
     * @param null|int|\DateInterval $ttl   Optional. The TTL value of this item. If no value is sent and
     *                                      the driver supports TTL then the library may set a default value
     *                                      for it or let the driver take care of that.
     *
     * @return bool True on success and false on failure.
     *
     * @throws \Psr\SimpleCache\InvalidArgumentException
     *   MUST be thrown if the $key string is not a legal value.
     */
    public function set($key, $value, $ttl = null){
        $this->validateKeyValue($key, $value);

        $bulk = new BulkWrite();
        $filter = ['_id' => (string) $key];
        $data = [
            'data' => $value,
            'expireAt' => new UTCDateTime(1000*(time()+$ttl)),
        ];
        $bulk->update($filter, $data, ['upsert' => true]);

        $result = $this->manager->executeBulkWrite($this->namespace, $bulk);
        return empty($result->getWriteErrors());
    }

    /**
     * Delete an item from the cache by its unique key.
     *
     * @param string $key The unique cache key of the item to delete.
     *
     * @return bool True if the item was successfully removed. False if there was an error.
     *
     * @throws \Psr\SimpleCache\InvalidArgumentException
     *   MUST be thrown if the $key string is not a legal value.
     */
    public function delete($key){
        $this->validateKeyValue($key);

        $bulk = new BulkWrite();
        $filter = ['_id' => (string) $key];

        $bulk->delete($filter, ['limit' => 1]);

        $result = $this->manager->executeBulkWrite($this->namespace, $bulk);

        return empty($result->getWriteErrors());
    }

    /**
     * Wipes clean the entire cache's keys.
     *
     * @return bool True on success and false on failure.
     */
    public function clear(){
        $bulk = new BulkWrite();

        $bulk->delete([], ['limit' => 0]);

        $result = $this->manager->executeBulkWrite($this->namespace, $bulk);
        return empty($result->getWriteErrors());
    }

    /**
     * Obtains multiple cache items by their unique keys.
     *
     * @param iterable $keys    A list of keys that can obtained in a single operation.
     * @param mixed    $default Default value to return for keys that do not exist.
     *
     * @return iterable A list of key => value pairs. Cache keys that do not exist or are stale will have $default as value.
     *
     * @throws \Psr\SimpleCache\InvalidArgumentException
     *   MUST be thrown if $keys is neither an array nor a Traversable,
     *   or if any of the $keys are not a legal value.
     */
    public function getMultiple($keys, $default = null){
        $keys = array_map(function($key){
            $this->validateKeyValue($key);
            return (string)$key;
        }, $keys);
        $filter= ['_id' => ['$in' =>$keys]];
        $query = new Query($filter);

        $cursor = $this->manager->executeQuery($this->namespace, $query);
        $map = array_fill_keys($keys, $default);

        foreach($cursor as $doc){
            $map[$doc->_id] = $doc->data ?? $default;
        }
        return $map;
    }

    /**
     * Persists a set of key => value pairs in the cache, with an optional TTL.
     *
     * @param iterable               $values A list of key => value pairs for a multiple-set operation.
     * @param null|int|\DateInterval $ttl    Optional. The TTL value of this item. If no value is sent and
     *                                       the driver supports TTL then the library may set a default value
     *                                       for it or let the driver take care of that.
     *
     * @return bool True on success and false on failure.
     *
     * @throws \Psr\SimpleCache\InvalidArgumentException
     *   MUST be thrown if $values is neither an array nor a Traversable,
     *   or if any of the $values are not a legal value.
     */
    public function setMultiple($values, $ttl = null){
        $bulk = new BulkWrite();
        $datetime = new UTCDateTime(1000*(time()+$ttl));

        foreach($values as $key => $value){
            $this->validateKeyValue($key, $value);

            $filter = ['_id' => (string) $key];
            $data = [
                'data' => $value,
                'expireAt' => $datetime,
            ];

            $bulk->update($filter, $data, ['upsert'=>true]);
        }

        $result = $this->manager->executeBulkWrite($this->namespace, $bulk);

        return empty($result->getWriteErrors());
    }

    /**
     * Deletes multiple cache items in a single operation.
     *
     * @param iterable $keys A list of string-based keys to be deleted.
     *
     * @return bool True if the items were successfully removed. False if there was an error.
     *
     * @throws \Psr\SimpleCache\InvalidArgumentException
     *   MUST be thrown if $keys is neither an array nor a Traversable,
     *   or if any of the $keys are not a legal value.
     */
    public function deleteMultiple($keys){
        $bulk = new BulkWrite();
        $keys = array_map(function($key) {
            $this->validateKeyValue($key);
            return (string)$key;
        }, $keys);
        $filter= ['_id' => ['$in' =>$keys]];

        $bulk->delete($filter, ['limit' => 0]);

        $result = $this->manager->executeBulkWrite($this->namespace, $bulk);

        return empty($result->getWriteErrors());
    }

    /**
     * Determines whether an item is present in the cache.
     *
     * NOTE: It is recommended that has() is only to be used for cache warming type purposes
     * and not to be used within your live applications operations for get/set, as this method
     * is subject to a race condition where your has() will return true and immediately after,
     * another script can remove it making the state of your app out of date.
     *
     * @param string $key The cache item key.
     *
     * @return bool
     *
     * @throws \Psr\SimpleCache\InvalidArgumentException
     *   MUST be thrown if the $key string is not a legal value.
     */
    public function has($key){
        $this->validateKeyValue($key);

        $query = new Query(['_id' => (string) $key], ['limit' => 1, 'projection' => ['_id' => 1]]);
        $cursor = $this->manager->executeQuery($this->namespace, $query);

        return !empty($cursor->toArray());
    }

    protected function validateKeyValue($key, $value = null){
        if (empty($key) || !is_scalar($key))
            throw new InvalidArgumentException('$key must be a scalar.');

        if ($value === null)
            return;
    }
}

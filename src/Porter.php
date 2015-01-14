<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2014 Vanilla Forums Inc.
 * @license MIT
 */

namespace MongoToMysql;

use MongoClient;
use MongoDB;
use MongoCollection;
use Garden\Db\Db;

class Porter {
    /// Properties ///

    protected $config = [];

    /**
     * @var Db
     */
    protected $db;

    /**
     * @var int
     */
    protected $limit;

    /**
     * @var int The maximum number of characters before a varchar is converted to text.
     */
    public $maxVarcharLength = 500;

    /**
     * @var MongoDB
     */
    protected $mongo;

    protected $allKeys = [];

    protected $tableCounts = [];

    /// Methods ///

    public function __construct($config) {
        $this->config = $config;
    }

    /**
     * @return Db
     */
    public function getDb() {
        if ($this->db === null) {
            $dbConfig = $this->config;
            array_touch('driver', $dbConfig, 'MySqlDb');
            $this->db = Db::create($dbConfig);
        }
        return $this->db;
    }

    /**
     * @param Db $db
     */
    public function setDb(Db $db) {
        $this->db = $db;
    }

    /**
     * Ensure that the destination table can accommodate a row.
     *
     * @param array $row The row to check.
     * @param string $tableName The name of the table.
     * @throws \Exception Throws an exception if a data type cannot be guessed or there is a problem with the database.
     */
    protected function ensureRowStructure($row, $tableName) {
        $tableDef = $this->getDb()->getTableDef($tableName);
        if (!$tableDef) {
            $tableDef = ['columns' => []];
        }

        $set = false;
        foreach ($row as $name => $value) {
            if (!array_key_exists($name, $tableDef['columns'])) {
                $tableDef['columns'][$name] = [
                    'type' => $this->guessDbType($value, $name, $row),
                    'required' => false,
                ];
                $set = true;
            } else {
                $type1 = $tableDef['columns'][$name]['type'];
                $type2 = $this->guessDbType($value);

                if ($type1 !== $type2) {
                    $tableDef['columns'][$name]['type'] = $this->guessCompareDbTypes(
                        $type1,
                        $type2
                    );
                }
            }
        }

        if ($set) {
            // Add the primary keys.
            if (isset($tableDef['columns']['_id'])) {
                $tableDef['indexes'][Db::INDEX_PK] = ['type' => Db::INDEX_PK, 'columns' => ['_id']];
            } elseif (isset($tableDef['columns']['_parentid']) && isset($tableDef['columns']['_index'])) {
                $tableDef['indexes'][Db::INDEX_PK] = ['type' => Db::INDEX_PK, 'columns' => ['_parentid', '_index']];
            }
            $this->getDb()->setTableDef($tableName, $tableDef);
        }
    }

    /**
     * Export a single mongo collection.
     *
     * @param MongoCollection $c The collection to export.
     * @throws \Exception Throws an exception if something goes wrong during the insert.
     */
    public function exportCollection(MongoCollection $c) {
        $tableName = $c->getName();
        echo static::ts()." Exporting collection $tableName:\n";
        $startTime = microtime(true);

        $total = $c->count();
//        $data = $c->find(["_key" => ['$regex' => '^group:cid:\d+:privileges']]);
        $data = $c->find();
        if ($this->getLimit()) {
            $data->limit($this->getLimit());
            $total = min($this->getLimit(), $total);
        }

        try {
            $count = 0;
            $lastPercent = 0;
            $lastTime = $startTime;
            foreach ($data as $row) {
                $exportTableName = $this->getImportTablename($row, $tableName);

                $row2 = $this->flattenArray($row);

                // Check for array columns.
                if (isset($row2['_arr'])) {
                    $arrays = $row2['_arr'];
                    unset($row2['_arr']);
                    foreach ($arrays as $akey => $arr) {
                        $this->exportCollectionArray($exportTableName, $row2['_id'], $akey, $arr);
                    }
                }

                if (count($row2) > 500) {
                    echo "  Skipping _id {$row2['_id']}, too many columns. (".count($row2).")\n";
                    $count++;
                    continue;
                }

                if (!isset($this->tableCounts[$exportTableName])) {
                    $this->tableCounts[$exportTableName] = 1;
                } else {
                    $this->tableCounts[$exportTableName]++;
                }
                $row2['_num'] = $this->tableCounts[$exportTableName];

                $this->ensureRowStructure($row2, $exportTableName);
                $this->getDb()->insert($exportTableName, $row2, [Db::OPTION_REPLACE => true]);


//                $p = $count / $total;
                $count++;
                $percent = $count / ($total !== 0 ? $total : 1);
                $now = microtime(true);
                $elapsed = $now - $startTime;
                $estimate = $elapsed / $percent;
                $left = format_timespan($estimate - $elapsed);

                $percent = round($percent * 100);

//                $estimate = format_timespan($estimate);
                if ($percent > $lastPercent && $now - $lastTime >= 10) {
                    echo "  $percent% ($count/$total, $left left)\n";
                    $lastPercent = $percent;
                    $lastTime = $now;
                }
            }
            $finishTime = microtime(true);
            $elapsed = format_timespan($finishTime - $startTime);
            echo "  Done. ($count rows in $elapsed)\n";
        } catch (\Exception $ex) {
            throw $ex;
        }


    }

    /**
     * Export a single array value into a child table.
     *
     * @param $parentTableName The name of the parent table.
     * @param $parentID The _id on the parent row.
     * @param $columnName The column name to export.
     * @param $arr The array to export.
     */
    protected function exportCollectionArray($parentTableName, $parentID, $columnName, $arr) {
        $childTableName = $parentTableName.'__'.$columnName;

        if (!isset($this->tableCounts[$childTableName])) {
            $this->tableCounts[$childTableName] = 0;
        }

        foreach ($arr as $i => $row) {
            if (is_array($row)) {
                $row2 = array_merge(['_parentid' => $parentID, '_index' => $i], $this->flattenArray($arr));
                unset($row2['_arr']); // don't support nested arrays
            } else {
                $row2 = ['_parentid' => $parentID, '_index' => $i, $columnName => $row];
            }

            $row2['_num'] = ++$this->tableCounts[$childTableName];

            $this->ensureRowStructure($row2, $childTableName);
            $this->getDb()->insert($childTableName, $row2, [Db::OPTION_REPLACE => true]);
        }
    }

    /**
     * Flatten a nested array into a one-dimensional array.
     *
     * @param $arr
     * @param string $path
     * @return array
     */
    protected function flattenArray($arr, $path = '') {
        $result = [];
        foreach ($arr as $k => $v) {
            if (is_array($v)) {
                if (isset($v[0]) || count($v) > 25) {
                    // Numeric and long arrays get stored in child tables.
                    $result['_arr'][$path.$k] = $v;
                    if ($k !== 'members') {
                        $foo = 'bar';
                    }
                } else {
                    $result = array_merge($result, $this->flattenArray($v, $path.$k.'_'));
                }
            } elseif ($v instanceof \MongoDate) {
                $dt = gmdate('c', $v->sec);
                $result[$path.$k] = $dt;
            } else {
                $result[$path.$k] = $v;
            }
        }
        return $result;
    }

    protected function getImportTablename($row, $tableName) {
        if (array_key_exists('_key', $row)) {
            $key = $row['_key'];
            // kludge for specific tables.
            if (preg_match('`^tag:(.+):topics$`', $key, $m)) {
                $key = 'tag_topics';
            } elseif (preg_match('`^group:cid:\d+:privileges.*:members$`', $key, $m)) {
                $key = 'group_privileges_members';
            } elseif (preg_match('`^group:cid:\d+:privileges`', $key, $m)) {
                $key = 'group_privileges';
            } elseif (preg_match('`^group:.*:members$`', $key, $m)) {
                $key = 'group_members';
            } elseif (preg_match('`^group:[^:]*$`', $key, $m)) {
                $key = 'group';
            } elseif (preg_match('`^ip:.*:uid$`', $key, $m)) {
                $key = 'ip_uid';
            }

            list($first) = explode(':', $key);
            if (in_array($first, ['settings', 'widgets'])) {
                $key = $first;
            }

            $key = preg_replace('`\d+`', '#', $key);
            $key = str_replace([':NaN:', ':undefined:'], ':#:', $key);
            $key = preg_replace('`[:# ]+`', '_', $key);
            $key = trim($key, '_');

            if (isset($this->allKeys[$key])) {
                $this->allKeys[$key]++;
            } else {
                $this->allKeys[$key] = 0;
            }

            if (!is_numeric($key)) {
                return $key;
            }
        }
        return $tableName;
    }

    /**
     * Export all of the tables in the mongodb.
     */
    public function exportCollections() {
        $collections = $this->getMongo()->listCollections(false);
        foreach ($collections as $c) {
            $this->exportCollection($c);
        }
    }

    /**
     * Try and guess a mongoDb type from its value.
     *
     * @param $value
     * @return string
     * @throws \Exception
     */
    public function guessDbType($value, $name = '', $row = []) {
        if ($value instanceof \MongoId) {
            return 'varchar(24)';
        } elseif ($value instanceof \MongoDate || $value instanceof \DateTime) {
            return 'datetime';
        } elseif (is_int($value) || is_null($value)) {
            return 'int';
        } elseif (is_double($value)) {
            return 'double';
        } elseif (is_string($value)) {
            $strlen = strlen($value);

            if ($strlen > $this->maxVarcharLength) {
                return 'text';
            } elseif (preg_match('`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,4})?`', $value)) {
                return 'datetime';
            } else {
                if ($strlen < 50) {
                    $strlen = 50;
                } elseif ($strlen < 100) {
                    $strlen = 100;
                } elseif ($strlen < 255) {
                    $strlen = 255;
                }
                return "varchar($strlen)";
            }
        } else {
            if ($name && $row) {
                $for = $name.' in '.json_encode($row);
            } else {
                $for = json_encode($value);
            }
            throw new \Exception("Unknown type for: ".$for);
        }
    }

    /**
     * Guess between two data types and return the most forgiving data type.
     *
     * @param string $type1
     * @param string $type2
     * @return string Returns the most forgiving type.
     */
    public function guessCompareDbTypes($type1, $type2) {
        $types = [$type1, $type2];
        asort($types);
        list($type1, $type2) = $types;

        if ($type1 === $type2) {
            return $type1;
        } elseif ($type1 === 'text' || $type2 === 'text') {
            return 'text';
        } elseif ($type1 === 'double' && $type2 === 'int') {
            return 'double';
        } elseif (str_begins($type1, 'varchar') && str_begins($type2, 'varchar')) {
            if (preg_match('`(\d+)`', $type1, $m1) && preg_match('`(\d+)`', $type2, $m2)) {
                $len1 = $m1[1];
                $len2 = $m2[1];

                return 'varchar('.max($len1, $len2).')';
            }
            return $type2;
        } else {
            return 'varchar(255)';
        }


    }

    /**
     * @return int
     */
    public function getLimit() {
        return $this->limit;
    }

    /**
     * @param int $limit
     */
    public function setLimit($limit) {
        $this->limit = $limit;
    }

    /**
     * @return MongoDB
     */
    public function getMongo() {
        if ($this->mongo === null) {
            $client = new MongoClient();
            $this->mongo = $client->selectDB(val('mdbname', $this->config));
        }
        return $this->mongo;
    }

    /**
     * @param MongoDB $mongo
     */
    public function setMongo(MongoDB $mongo) {
        $this->mongo = $mongo;
    }

    /**
     * Export the mongoDb.
     */
    public function run() {
        $this->tableCounts = [];
        $this->exportCollections();
    }

    public static function ts() {
        return '['.strftime('%Y-%m-%d %r').']';
    }
}

<?php
namespace ElasticSearch\tests\units;

require_once __DIR__ . '/../Base.php';

use ElasticSearch\tests\Helper;

class MockTransport {
    public function request ($path, $method="GET", $payload=false) {
        $response = [
            'took' => 3,
            'items' => [],
            'errors' => false,
        ];

        foreach(array_chunk(explode("\n", trim($payload)), 2) as $operation) {
            $command = json_decode($operation[0], TRUE);

            $metadata = array_values($command)[0];
            if (array_key_exists('create', $command)) {
                $metadata['status'] = 409;
                $metadata['error'] = 'Document Already Exists';
                $response['errors'] = true;
            } else {
                $metadata['status'] = 200;
                $metadata['_version'] = 1;
            }

            $response['items'][] = [ array_keys($command)[0] => $metadata];
        }

        return $response;
    }
}

class Bulk extends \ElasticSearch\tests\Base {

    public function testCommit_singleRequestWithError () {
        $bulk = new \ElasticSearch\Bulk(new MockTransport(), 'quizlet', 'test', $chunk_size = 3);
        $bulk->index([ 'name' => 'payload to insert 1' ]);
        $bulk->create([ 'name' => 'payload to create' ]);
        $bulk->index([ 'name' => 'payload to insert 2' ]);

        $result = $bulk->commit();

        $this->boolean($result['errors'])->isTrue();
        $this->array($result)->hasKey('items');

        $item = $result['items'][0];
        $this->array($item)->hasKey('index');
        $this->array($item['index'])->notHasKey('error');
        $this->integer($item['index']['status'])->isEqualTo(200);

        $item = $result['items'][1];
        $this->array($item)->hasKey('create');
        $this->array($item['create'])->hasKey('error');
        $this->integer($item['create']['status'])->isEqualTo(409);

        $item = $result['items'][2];
        $this->array($item)->hasKey('index');
        $this->array($item['index'])->notHasKey('error');
        $this->integer($item['index']['status'])->isEqualTo(200);
    }

    public function testCommit_chunkedRequestWithError () {
        $bulk = new \ElasticSearch\Bulk(new MockTransport(), 'quizlet', 'test', $chunk_size = 1);
        $bulk->index([ 'name' => 'payload to insert 1' ]);
        $bulk->create([ 'name' => 'payload to create' ]);
        $bulk->index([ 'name' => 'payload to insert 2' ]);

        $result = $bulk->commit();

        $this->boolean($result['errors'])->isTrue();
        $this->array($result)->hasKey('items');

        $item = $result['items'][0];
        $this->array($item)->hasKey('index');
        $this->array($item['index'])->notHasKey('error');
        $this->integer($item['index']['status'])->isEqualTo(200);

        $item = $result['items'][1];
        $this->array($item)->hasKey('create');
        $this->array($item['create'])->hasKey('error');
        $this->integer($item['create']['status'])->isEqualTo(409);

        $item = $result['items'][2];
        $this->array($item)->hasKey('index');
        $this->array($item['index'])->notHasKey('error');
        $this->integer($item['index']['status'])->isEqualTo(200);
    }

    public function testCommit_singleRequestSuccess () {
        $bulk = new \ElasticSearch\Bulk(new MockTransport(), 'quizlet', 'test', $chunk_size = 2);
        $bulk->index([ 'name' => 'payload to insert 1' ]);
        $bulk->index([ 'name' => 'payload to insert 2' ]);

        $result = $bulk->commit();

        $this->boolean($result['errors'])->isFalse();
        $this->array($result)->hasKey('items');

        $item = $result['items'][0];
        $this->array($item)->hasKey('index');
        $this->array($item['index'])->notHasKey('error');
        $this->integer($item['index']['status'])->isEqualTo(200);

        $item = $result['items'][1];
        $this->array($item)->hasKey('index');
        $this->array($item['index'])->notHasKey('error');
        $this->integer($item['index']['status'])->isEqualTo(200);
    }

    public function testCommit_chunkedRequestSuccess () {
        $bulk = new \ElasticSearch\Bulk(new MockTransport(), 'quizlet', 'test', $chunk_size = 1);
        $bulk->index([ 'name' => 'payload to insert 1' ]);
        $bulk->index([ 'name' => 'payload to insert 2' ]);

        $result = $bulk->commit();

        $this->boolean($result['errors'])->isFalse();
        $this->array($result)->hasKey('items');

        $item = $result['items'][0];
        $this->array($item)->hasKey('index');
        $this->array($item['index'])->notHasKey('error');
        $this->integer($item['index']['status'])->isEqualTo(200);

        $item = $result['items'][1];
        $this->array($item)->hasKey('index');
        $this->array($item['index'])->notHasKey('error');
        $this->integer($item['index']['status'])->isEqualTo(200);
    }
}

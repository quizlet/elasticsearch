<?php // vim:set ts=4 sw=4 et:

namespace ElasticSearch\Transport;

/**
 * This file is part of the ElasticSearch PHP client
 *
 * (c) Raymond Julin <raymond.julin@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

if (!defined('CURLE_OPERATION_TIMEDOUT'))
    define('CURLE_OPERATION_TIMEDOUT', 28);


class HTTP extends Base {
    /**
     * curl handler which is needed for reusing existing http connection to the server
     * @var resource
     */
    protected $ch;

    private $mirror;
    private $mirror_suffix;


    public function __construct($connections, $mirror, $mirror_suffix) {
        parent::__construct($connections);
        $this->ch = curl_init();
        $this->mirror = $mirror;
        $this->mirror_suffix = $mirror_suffix;
    }

    /**
     * Index a new document or update it if existing
     *
     * @return array
     * @param array $document
     * @param mixed $id Optional
     * @param array $options
     */
    public function index($document, $id=false, array $options = array()) {
        $url = $this->buildUrl(array($this->type, $id), $options);
        $method = ($id == false) ? "POST" : "PUT";
        if ($this->mirror) {
            $first = $this->call($url, $method, $document);
            $index = $this->getIndex();
            $this->setIndex($index . $this->mirror_suffix);
            $url = $this->buildUrl(array($this->type, $id), $options);
            $second = $this->call($url, $method, $document);
            $this->setIndex($index);
            return $first && $second;
        }
        return $this->call($url, $method, $document);
    }

    /**
     * Search
     *
     * @return array
     * @param array|string $query
     * @param array $options
     */
    public function search($query, array $options = array()) {
        if (is_array($query)) {
            /**
             * Array implies using the JSON query DSL
             */
            $url = $this->buildUrl(array(
                $this->type, "_search"
            ));
            $result = $this->call($url, "GET", $query);
        }
        elseif (is_string($query)) {
            /**
             * String based search means http query string search
             */
            $url = $this->buildUrl(array(
                $this->type, "_search?q=" . $query
            ));
            $result = $this->call($url, "POST", $options);
        }
        return $result;
    }

    /**
     * Multi search
     * 
     * @return array
     * @param array $searches
     */
    public function multiSearch($request) {
        $url = $this->buildUrl('/_msearch');
        $result = $this->call($url, "GET", $request);
        return $result;
    }

    /**
     * Delete
     *
     * @return array
     * @param mixed $query
     * @param array $options Parameters to pass to delete action
     */
    public function deleteByQuery($query, array $options = array()) {
        $options += array(
            'refresh' => true
        );
        if (is_array($query)) {
            /**
             * Array implies using the JSON query DSL
             */
            $url = $this->buildUrl(array($this->type, "_query"));
            $result = $this->call($url, "DELETE", $query);
        }
        elseif (is_string($query)) {
            /**
             * String based search means http query string search
             */
            $url = $this->buildUrl(array($this->type, "_query"), array('q' => $query));
            $result = $this->call($url, "DELETE");
        }
        if ($options['refresh']) {
            $this->request('_refresh', "POST");
        }
        return !isset($result['error']) && $result['ok'];
    }

    /**
     * Perform a request against the given path/method/payload combination
     * Example:
     * $es->request('/_status');
     *
     * @param string|array $path
     * @param string $method
     * @param array|string|bool $payload
     * @return array
     */
    public function request($path, $method="GET", $payload=false) {
        return $this->call($this->buildUrl($path), $method, $payload);
    }

    /**
     * Flush this index/type combination
     *
     * @return array
     * @param mixed $id Id of document to delete
     * @param array $options Parameters to pass to delete action
     */
    public function delete($id=false, array $options = array()) {
        if ($id) {
            if ($this->mirror) {
                $first = $this->request(array($this->type, $id), "DELETE");
                $index = $this->getIndex();
                $this->setIndex($index . $this->mirror_suffix);
                $second = $this->request(array($this->type, $id), "DELETE");
                $this->setIndex($index);
                return $first && $second;
            }
            return $this->request(array($this->type, $id), "DELETE");
        } else {
            if ($this->mirror) {
                $first = $this->request(false, "DELETE");
                $index = $this->getIndex();
                $this->setIndex($index . $this->mirror_suffix);
                $second = $this->request(false, "DELETE");
                $this->setIndex($index);
                return $first && $second;
            }
            return $this->request(false, "DELETE");
        }
    }

    /**
     * Perform a http call against an url with an optional payload
     *
     * @return array
     * @param string $url
     * @param string $method (GET/POST/PUT/DELETE)
     * @param array|string|bool $payload The document/instructions to pass along
     * @throws HTTPException
     */
    protected function call($url, $method="GET", $payload=false) {
        $conn = $this->ch;
        $protocol = "http";

        $connections = $this->connections;
        shuffle($connections);
        $n = count($connections);
        $retry_count = 0;
        for ($i = 0; $i < $n; $i++) {
            $connection = $connections[$i];
            $requestURL = $protocol . "://" . $connection['host'] . ':' . $connection['port'] . $url;
            curl_setopt($conn, CURLOPT_URL, $requestURL);
            curl_setopt($conn, CURLOPT_CONNECTTIMEOUT_MS, 50);
            curl_setopt($conn, CURLOPT_TIMEOUT, 3);
            curl_setopt($conn, CURLOPT_PORT, $connection['port']);
            curl_setopt($conn, CURLOPT_RETURNTRANSFER, 1);
            curl_setopt($conn, CURLOPT_CUSTOMREQUEST, strtoupper($method));
            curl_setopt($conn, CURLOPT_FORBID_REUSE, 0);

            if ((is_array($payload) && count($payload) > 0) || is_string($payload) && $payload !== "")
                curl_setopt($conn, CURLOPT_POSTFIELDS, is_string($payload) ? $payload : json_encode($payload));
            else
                curl_setopt($conn, CURLOPT_POSTFIELDS, null);

            $response = curl_exec($conn);
            if ($response !== false) {
                $data = json_decode($response, true);
                if (!$data) {
                    $data = array('error' => $response, "code" => curl_getinfo($conn, CURLINFO_HTTP_CODE));
                }
                return $data;
            }
            else {
                $errno = curl_errno($conn);
                switch ($errno)
                {
                    case CURLE_UNSUPPORTED_PROTOCOL:
                        $error = "Unsupported protocol [$protocol]";
                        break;
                    case CURLE_FAILED_INIT:
                        $error = "Internal cUrl error?";
                        break;
                    case CURLE_URL_MALFORMAT:
                        $error = "Malformed URL [$requestURL] -d " . json_encode($payload);
                        break;
                    case CURLE_COULDNT_RESOLVE_PROXY:
                        $error = "Couldnt resolve proxy";
                        break;
                    case CURLE_COULDNT_RESOLVE_HOST:
                        $error = "Couldnt resolve host";
                        break;
                    case CURLE_COULDNT_CONNECT:
                        $retry = true;
                        $error = "Couldnt connect to host [{$connection['host']}], ElasticSearch down?";
                        break;
                    case CURLE_OPERATION_TIMEDOUT:
                        $error = "Operation timed out on [$requestURL]";
                        if ($retry_count < 3) {
                            $retry_count++;
                            $i--;
                            usleep(1000000);
                        } else {
                            $retry_count = 0;
                        }
                        continue;
                    default:
                        $error = "Unknown error";
                        if ($errno == 0)
                            $error .= ". Non-cUrl error";
                        break;
                }
            }
        }
        $exception = new HTTPException($error);
        $exception->payload = $payload;
        $exception->protocol = $protocol;
        $exception->method = $method;
        throw $exception;
    }
}

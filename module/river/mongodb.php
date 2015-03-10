<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4: */
/**
 * River - mongodb
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 * PHP 5.5 Later
 * 
 * @category   module
 * @package    river
 * @author     Junyong Park
 * @copyright  2014
 * @version    SVN: $Id:$
 */

namespace mongodb;

require_once(MOD_DIR . 'RiverInterface.php');


class River implements \RiverInterface
{
    /**
     * _dsn
     * 
     * @access protected
     * @var    array
     */
    protected $_dsn = [];

    /**
     * _query
     * 
     * @access protected
     * @var    string
     */
    protected $_query = NULL;

    /**
     * _client
     * 
     * @access protected
     * @var    \MongoClient
     */
    protected $_client = NULL;

    /**
     * _cursor
     * 
     * @access protected
     * @var    string
     */
    protected $_cursor = NULL;


    /**
     * Constructor
     * 
     * @access public
     * @param  array
     * @param  string
     * @return void
     * @throws \UnexpectedValueException
     */
    public function __construct( array $dsn, $query )
    {
        // check parameter
        if( !strlen(trim($query)) ) throw new \UnexpectedValueException('invalid query');

        $this->_dsn = $dsn;
        $this->_query = $query;
    }

    /**
     * open
     * 
     * @access public
     * @return boolean
     * @throws \UnexpectedValueException
     * @throws \MongoException
     */
    public function open()
    {
        // create MongoClient instance
        $this->_client = new \MongoClient(sprintf('mongodb://%s:%s', @$this->_dsn['host'], @$this->_dsn['port']));

        // evaluate query
        if( !($query = $this->_evaluateQuery()) ) throw new \UnexpectedValueException('Fail to evaluate query');

        // cursor
        $this->_cursor = new \MongoCommandCursor($this->_client, @$this->_dsn['dbname'].'.*', json_decode($query, TRUE));

        return TRUE;
    }

    /**
     * fetch
     * 
     * @access public
     * @return array
     */
    public function fetch()
    {
        if( !$this->_cursor instanceof \MongoCursorInterface ) throw new \LogicException('Invalid cursor');

        /*
         * generatorでsegfaultが起きるためiterator_to_array()でcursorを変更する
         * 環境：PHP-5.5.14, mongo-1.5.4
         * TODO 問題なかったらiterator_to_array()取り除く
         */
        foreach( iterator_to_array($this->_cursor) as $row )
        {
            $ctrl = (yield $row);
//             print_r($ctrl);
        }
    }

    /**
     * abort
     * 
     * @access public
     * @return boolean
     * @throws \LogicException
     * @throws \MongoException
     */
    public function abort()
    {
        if( !$this->_cursor instanceof \MongoCursorInterface ) throw new \LogicException('Invalid cursor');

        // Now the cursor is valid, so we can get the hash and ID out:
        $info = $this->_cursor->info();

        // Kill the cursor
        return $this->_client->killCursor($info['server'], $info['id']);
    }

    /**
     * options
     * 
     * @access public
     * @return boolean
     */
    public function options( array $option )
    {
        return FALSE;
    }

    /**
     * close
     * 
     * @access public
     * @return boolean
     * @throws \MongoException
     */
    public function close()
    {
        /*
         * x64で$this->_client->close()をするとSegmentation fault (core dumped)が起きるため
         * closeせずにreturnする。
         * 環境：PHP-5.5.14, mongo-1.5.4
         * TODO 問題なかったらclose()させる。
         */
        if( PHP_INT_SIZE == 8 ) return TRUE; // compiled to 64bit

        $connections = $this->_client->getConnections();
        if( !is_array($connections) ) return FALSE;

        foreach( $connections as $conn )
        {
            if( !$this->_client->close($conn['hash']) ) return FALSE;
        }

        return TRUE;
    }

    /**
     * _evaluateQuery
     * 
     * @access private
     * @return string
     * @throws \UnexpectedValueException
     */
    private function _evaluateQuery()
    {
        return preg_replace_callback(
            '/<\?php [^?>]+\?>/is',
            function( $matches )
            {
                if( @eval('$ret=' . rtrim(str_replace(['<?php ', '?>'], '', $matches[0]), ';') . ';') === FALSE )
                    throw new \UnexpectedValueException('Fail to evaluate query');

                return $ret;
            },
            $this->_query
        );
    }
}

<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4: */
/**
 * River - mysql
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

namespace mysql;

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
     * _stmt
     * 
     * @access private
     * @var    \PDOStatement
     */
    private $_stmt = NULL;

    /**
     * _connId
     * 
     * @access private
     * @var    string
     */
    private $_connId = NULL;


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
        if( !strlen(trim($query)) ) throw new \UnexpectedValueException('invalid query(SQL)');

        $this->_dsn = $dsn;
        $this->_query = $query;
    }

    /**
     * open
     * 
     * @access public
     * @return boolean
     * @throws \PDOException
     */
    public function open()
    {
        // DB Handler
        $dbh = $this->_getDBHandler();

        // set dbh options
        $dbh->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        $dbh->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, FALSE);

        // get connection_id(thread_id)
        @list($this->_connId) = $dbh->query('SELECT CONNECTION_ID()')->fetch(\PDO::FETCH_NUM);

        // query
        $this->_stmt = $dbh->query($this->_query);
        $this->_stmt->setFetchMode(\PDO::FETCH_NUM);

        return TRUE;
    }

    /**
     * fetch
     * 
     * @access public
     * @return array
     * @throws \LogicException
     * @throws \PDOStatement
     */
    public function fetch()
    {
        if( !$this->_stmt instanceof \PDOStatement ) throw new \LogicException('Invalid statement');

        while( $row = $this->_stmt->fetch() )
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
     * @throws \PDOStatement 
     */
    public function abort()
    {
        if( !strlen($this->_connId) ) throw new \LogicException('Invalid connection ID, Did you open database?');

        // DB Handler
        $dbh = $this->_getDBHandler();

        // set dbh options
        $dbh->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        // kill db thread
        $stmt = $dbh->query('KILL ' . $this->_connId);

        // close
        return $stmt->closeCursor();
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
     * @throws \LogicException
     */
    public function close()
    {
        if( !$this->_stmt instanceof \PDOStatement ) throw new \LogicException('Invalid statement');

        return $this->_stmt->closeCursor();
    }

    /**
     * _getDBHandler
     * 
     * @access private
     * @return \PDO
     */
    private function _getDBHandler()
    {
        static $dsn;

        // set dsn
        $dsn = sprintf('%s:host=%s;port=%s;dbname=%s;', @$this->_dsn['driver'], @$this->_dsn['host'], @$this->_dsn['port'], @$this->_dsn['dbname']);
        if( isset($this->_dsn['charset']) && strlen($this->_dsn['charset']) ) $dsn .= 'charset='. $this->_dsn['charset'] . ';';

        // create PDO instance(db handler)
        return new \PDO($dsn, @$this->_dsn['user'], @$this->_dsn['passwd']);
    }
}

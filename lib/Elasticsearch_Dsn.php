<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4: */
/**
 * Elasticsearch - DSN
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 * PHP 5.4 Later
 * 
 * @category   lib
 * @package    Elasticsearch
 * @author     Junyong Park
 * @copyright  2013
 * @version    SVN: $Id:$
 */


class Elasticsearch_Dsn
{
    /**
     * _hosts
     * 
     * @access private
     * @var    array
     */
    private $_hosts = [];

    /**
     * _baseIndex
     * 
     * @access private
     * @var    string
     */
    private $_baseIndex = NULL;

    /**
     * _indexNumber
     * 
     * @access private
     * @var    integer
     */
    private $_indexNumber = 0;

    /**
     * _alias
     * 
     * @access private
     * @var    string
     */
    private $_alias = NULL;

    /**
     * _param
     * 
     * @access private
     * @var    string
     */
    private $_param = NULL;

    /**
     * $_rr
     * 
     * @access private
     * @var    boolean
     */
    private $_rr = FALSE;

    /**
     * _hostsCnt
     * 
     * @access private
     * @var    integer
     */
    private $_hostsCnt = 0;

    /**
     * _rrCnt
     * 
     * @access private
     * @var    integer
     */
    private $_rrCnt = 0;


    /**
     * Construct
     * 
     * @access public
     * @param  array $dsn [Optional]
     */
    public function __construct( array $dsn=[] )
    {
        // set dsn
        $this->setHosts(@$dsn['hosts']);
        $this->setBaseIndex(@$dsn['baseindex']);
        $this->setAlias(@$dsn['alias']);
        $this->setParam(@$dsn['param']);
    }

    /**
     * setHosts
     * 
     * @access public
     * @param  array $hosts
     */
    public function setHosts( $hosts )
    {
        $this->_hosts = explode(',', $hosts);
        $this->_hostsCnt = count($this->_hosts);

        return TRUE;
    }

    /**
     * getHost
     * 
     * @access public
     * @param  integer
     * @return array
     */
    public function getHost( $offset )
    {
        if( $this->_rr )
        {
            $ret = @$this->_hosts[($this->_rrCnt + $offset) % $this->_hostsCnt];

            $this->_rrCnt++;
            if( $this->_rrCnt == $this->_hostsCnt ) $this->_rrCnt = 0;
        }
        else $ret = @$this->_hosts[$offset];

        return $ret;
    }

    /**
     * setBaseIndex
     * 
     * @access public
     * @param  string $index
     * @return boolean
     */
    public function setBaseIndex( $index )
    {
        if( !is_string($index) || !strlen($index) ) return FALSE;

        $this->_baseIndex = $index;

        return TRUE;
    }

    /**
     * getBaseIndex
     * 
     * @access public
     * @return string
     */
    public function getBaseIndex()
    {
        return $this->_baseIndex;
    }

    /**
     * setIndexNumber
     * 
     * @access public
     * @param  integer $number
     * @return boolean
     */
    public function setIndexNumber( $number )
    {
        if( !is_numeric($number) ) return FALSE;

        $this->_indexNumber = $number;

        return TRUE;
    }

    /**
     * getIndexNumber
     * 
     * @access public
     * @return integer
     */
    public function getIndexNumber()
    {
        return $this->_indexNumber;
    }

    /**
     * getIndex
     * 
     * @access public
     * @return string
     */
    public function getIndex()
    {
        return $this->_baseIndex . '.' . $this->_indexNumber;
    }

    /**
     * setAlias
     * 
     * @access public
     * @param  string $alias
     * @return boolean
     */
    public function setAlias( $alias )
    {
        if( !is_string($alias) || !strlen($alias) ) return FALSE;

        $this->_alias = $alias;

        return TRUE;
    }

    /**
     * getAlias
     * 
     * @access public
     * @return string
     */
    public function getAlias()
    {
        return $this->_alias;
    }

    /**
     * setParam
     * 
     * @access public
     * @param  string $param
     * @return boolean
     */
    public function setParam( $param )
    {
        if( !is_string($param) || !strlen($param) ) return FALSE;

        $this->_param = $param;

        return TRUE;
    }

    /**
     * getParam
     * 
     * @access public
     * @return string
     */
    public function getParam()
    {
        return $this->_param;
    }

    /**
     * setRoundRobin
     * 
     * @access public
     * @param  boolean $rr
     * @return boolean
     */
    public function setRoundRobin( $rr )
    {
        $this->_rr = $rr ? TRUE : FALSE;

        return TRUE;
    }

    /**
     * getRoundRobin
     * 
     * @access public
     * @return boolean
     */
    public function getRoundRobin()
    {
        return $this->_rr;
    }
}

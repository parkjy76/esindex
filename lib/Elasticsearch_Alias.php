<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4: */
/**
 * Elasticsearch - Alias
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

require_once('Elasticsearch_Requester.php');


class Elasticsearch_Alias
{
    /**
     * _esDsn
     * 
     * @access private
     * @var    Elasticsearch_Dsn
     */
    private $_esDsn = NULL;

    /**
     * _requester
     * 
     * @access private
     * @var    Elasticsearch_Requester
     */
    private $_requester = NULL;


    /**
     * Construct
     * 
     * @access public
     * @param  Elasticsearch_Dsn $esDsn
     */
    public function __construct( Elasticsearch_Dsn $esDsn )
    {
        $this->_esDsn = $esDsn;
        $this->_requester = new Elasticsearch_Requester;
    }

    /**
     * getCurrentIndexNumber
     * 
     * @access public
     * @param  integer $offset [Optional]
     * @return Ambigous <NULL, integer>
     */
    public function getCurrentIndexNumber( $offset=0 )
    {
        $this->_requester->open();

        $ret = $this->_requester->get($this->_esDsn->getHost($offset) . '/_alias/' . $this->_esDsn->getAlias(), NULL);
        $data = json_decode($ret, TRUE);
        if( isset($data['error']) ) $ret = NULL;
        else
        {
            $indexes = array_keys($data);
            if( !count($indexes) ) $ret = NULL;
            else $ret = (int)ltrim(str_replace($this->_esDsn->getBaseIndex(), '', array_pop($indexes)), '.');
        }

        $this->_requester->close();

        return $ret;
    }

    /**
     * getToggleIndexNumber
     * 
     * @access public
     * @param  integer $var
     * @return 0|1
     */
    public function getToggleIndexNumber( $var )
    {
        if( !strlen($var) ) return 0;
 
        return (int)($var xor 1);
    }

    /**
     * alterIndex
     * 
     * @access public
     * @param  mixed $from
     * @param  mixed $to
     * @param  integer $offset [Optional]
     * @throws Exception
     * @return boolean|Ambigous <boolean, mixed>
     */
    public function alterIndex( $from, $to, $offset=0 )
    {
        $actions = [];

        if( strlen($to) )
            $actions[] = sprintf('{"add":{"index":"%s.%s", "alias":"%s"}}', $this->_esDsn->getBaseIndex(), $to, $this->_esDsn->getAlias());

        if( strlen($from) )
            $actions[] = sprintf('{"remove":{"index":"%s.%s", "alias":"%s"}}', $this->_esDsn->getBaseIndex(), $from, $this->_esDsn->getAlias());

        if( !count($actions) ) throw new InvalidArgumentException(__METHOD__ . ' invalid parameter');

        $this->_requester->open();

        $ret = $this->_requester->post($this->_esDsn->getHost($offset) . '/_aliases', sprintf('{"actions":[%s]}', implode(',', $actions)));
        $data = json_decode($ret, TRUE);
        if( isset($data['error']) ) throw new Exception(sprintf(__METHOD__ . ' (%s) %s', $data['status'], $data['error']));
        else $ret = TRUE;

        $this->_requester->close();

        return $ret;
    }
}

<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4: */
/**
 * Elasticsearch_Requester
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


class Elasticsearch_Requester
{
    /**
     * _cr
     * 
     * @access private
     * @var    resource
     */
    private $_cr = NULL;


    /**
     * open
     * 
     * @access public
     * @return boolean
     */
    public function open()
    {
        $cr = curl_init();
        if( !$cr ) return FALSE;
        $this->_cr = $cr;

        $options = [
            CURLOPT_CONNECTTIMEOUT => 5,
            CURLOPT_TIMEOUT => 10,
            CURLOPT_RETURNTRANSFER => 1,
            CURLOPT_BINARYTRANSFER => 1,
        ];

        return $this->setOptions($options);
    }

    /**
     * put
     * 
     * @access public
     * @param  string $url
     * @param  string $data
     * @param  boolean $chkErr [Optional]
     * @return boolean|mixed
     */
    public function put( $url, $data, $chkErr=TRUE )
    {
        $options = [
            CURLOPT_CUSTOMREQUEST => 'PUT',
            CURLOPT_URL => $url,
            CURLOPT_POSTFIELDS => $data,
        ];

        return $this->_send($options, $chkErr);
    }

    /**
     * post
     * 
     * @access public
     * @param  string $url
     * @param  string $data
     * @param  boolean $chkErr [Optional]
     * @return boolean|mixed
     */
    public function post( $url, $data, $chkErr=TRUE )
    {
        $options = [
            CURLOPT_CUSTOMREQUEST => 'POST',
            CURLOPT_URL => $url,
            CURLOPT_POSTFIELDS => $data,
        ];

        return $this->_send($options, $chkErr);
    }

    /**
     * get
     * 
     * @access public
     * @param  string $url
     * @param  string $data
     * @param  boolean $chkErr [Optional]
     * @return boolean|mixed
     */
    public function get( $url, $data, $chkErr=TRUE )
    {
        $options = [
            CURLOPT_CUSTOMREQUEST => 'GET',
            CURLOPT_URL => $url,
            CURLOPT_POSTFIELDS => $data,
        ];

        return $this->_send($options, $chkErr);
    }

    /**
     * delete
     * 
     * @access public
     * @param  string $url
     * @param  string $data
     * @param  boolean $chkErr [Optional]
     * @return boolean|mixed
     */
    public function delete( $url, $data, $chkErr=TRUE )
    {
        $options = [
            CURLOPT_CUSTOMREQUEST => 'DELETE',
            CURLOPT_URL => $url,
            CURLOPT_POSTFIELDS => $data,
        ];

        return $this->_send($options, $chkErr);
    }

    /**
     * head
     * 
     * @access public
     * @param  string $url
     * @param  boolean $chkErr [Optional]
     * @return boolean|mixed
     */
    public function head( $url, $chkErr=TRUE )
    {
        $options = [
            CURLOPT_NOBODY => TRUE,
            CURLOPT_URL => $url,
        ];

        return $this->_send($options, $chkErr);
    }

    /**
     * request
     * 
     * @access public
     * @param  array $options
     * @param  boolean $chkErr [Optional]
     * @return boolean|mixed
     */
    public function request( array $options, $chkErr=TRUE )
    {
        return $this->_send($options, $chkErr);
    }

    /**
     * _send
     * 
     * @access private
     * @param  array $options
     * @param  boolean $chkErr
     * @throws InvalidArgumentException
     * @throws ErrorException
     * @return boolean|mixed
     */
    private function _send( array $options, $chkErr )
    {
        // set options
        if( !$this->setOptions($options) ) return FALSE;

        $ret = curl_exec($this->_cr);

        // check error
        if( $chkErr && curl_errno($this->_cr) )
            throw new ErrorException(__METHOD__ . ' ' . curl_error($this->_cr) . '(' . curl_errno($this->_cr) . ')');

        return $ret;
    }

    /**
     * setOptions
     * 
     * @access public
     * @param  array $options
     * @return boolean
     * @throws InvalidArgumentException
     */
    public function setOptions( array $options )
    {
        // check curl resource
        if( !is_resource($this->_cr) ) throw new InvalidArgumentException(__METHOD__ . ' curl did not initiate');

        return curl_setopt_array($this->_cr, $options);
    }

    /**
     * getInfo
     * 
     * @access public
     * @return mixed
     * @throws InvalidArgumentException
     */
    public function getInfo()
    {
        // check curl resource
        if( !is_resource($this->_cr) ) throw new InvalidArgumentException(__METHOD__ . ' curl did not initiate');

        return curl_getinfo($this->_cr);
    }

    /**
     * close
     * 
     * @access public
     * @return boolean
     * @throws InvalidArgumentException
     */
    public function close()
    {
        // check curl resource
        if( !is_resource($this->_cr) ) throw new InvalidArgumentException(__METHOD__ . ' curl did not initiate');

        return curl_close($this->_cr);
    }
}

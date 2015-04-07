<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4: */
/**
 * Elasticsearch - Index
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


class Elasticsearch_Index
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
     * exist
     * 
     * @access public
     * @param  integer $offset [Optional]
     * @return boolean
     */
    public function exist( $offset=0 )
    {
        $this->_requester->open();

        $this->_requester->head($this->_getUrl(NULL, $offset));
        $status = $this->_requester->getInfo();

        $this->_requester->close();

        if( $status['http_code'] == 200 ) return TRUE;
        else return FALSE;
    }

    /**
     * getState
     * 
     * @access public
     * @param  interger $offset [Optional]
     * @return Ambigous <string, boolean, mixed>
     */
    public function getState( $offset=0 )
    {
        $this->_requester->open();

        $ret = $this->_requester->get($this->_getUrl('_settings/state', $offset), NULL);
        $data = json_decode($ret, TRUE);
        if( isset($data['error']) )
        {
            switch( $data['status'] )
            {
                case 403 :
                    $ret = 'close';
                    break;

                case 404 :
                    $ret = 'none';
                    break;
            }
        }
        else $ret = 'open';

        $this->_requester->close();

        return $ret;
    }

    /**
     * create
     * 
     * @access public
     * @param  string $json
     * @param  interger $offset [Optional]
     * @throws Exception
     * @return boolean
     */
    public function create( $json, $offset=0 )
    {
        $this->_requester->open();

        $ret = $this->_requester->put($this->_getUrl(NULL, $offset), $json);
        $data = json_decode($ret, TRUE);
        if( isset($data['error']) ) throw new Exception(sprintf(__METHOD__ . ' (%s) %s', $data['status'], $data['error']));
        else $ret = TRUE;

        $this->_requester->close();

        return $ret;
    }

    /**
     * delete
     * 
     * @access public
     * @param  interger $offset [Optional]
     * @throws Exception
     * @return Ambigous <boolean, mixed>
     */
    public function delete( $offset=0 )
    {
        $this->_requester->open();

        $ret = $this->_requester->delete($this->_getUrl(NULL, $offset), NULL);
        $data = json_decode($ret, TRUE);
        if( isset($data['error']) ) throw new Exception(sprintf(__METHOD__ . ' (%s) %s', $data['status'], $data['error']));
        else $ret = TRUE;

        $this->_requester->close();

        return $ret;
    }

    /**
     * open
     * 
     * @access public
     * @param  interger $offset [Optional]
     * @throws Exception
     * @return Ambigous <boolean, mixed>
     */
    public function open( $offset=0 )
    {
        $this->_requester->open();

        $ret = $this->_requester->post($this->_getUrl('_open', $offset), NULL);
        $data = json_decode($ret, TRUE);
        if( isset($data['error']) ) throw new Exception(sprintf(__METHOD__ . ' (%s) %s', $data['status'], $data['error']));
        else $ret = TRUE;

        $this->_requester->close();

        return $ret;
    }

    /**
     * close
     * 
     * @access public
     * @param  interger $offset [Optional]
     * @throws Exception
     * @return Ambigous <boolean, mixed>
     */
    public function close( $offset=0 )
    {
        $this->_requester->open();

        $ret = $this->_requester->post($this->_getUrl('_close', $offset), NULL);
        $data = json_decode($ret, TRUE);
        if( isset($data['error']) ) throw new Exception(sprintf(__METHOD__ . ' (%s) %s', $data['status'], $data['error']));
        else $ret = TRUE;

        $this->_requester->close();

        return $ret;
    }

    /**
     * flush
     * 
     * @access public
     * @param  interger $offset [Optional]
     * @throws Exception
     * @return boolean
     */
    public function flush( $offset=0 )
    {
        $this->_requester->open();

        $ret = $this->_requester->post($this->_getUrl('_flush', $offset), NULL);
        $data = json_decode($ret, TRUE);
        if( isset($data['error']) ) throw new Exception(sprintf(__METHOD__ . ' (%s) %s', $data['status'], $data['error']));
        else $ret = TRUE;

        $this->_requester->close();

        return $ret;
    }

    /**
     * clearCache
     * 
     * @access public
     * @param  interger $offset [Optional]
     * @throws Exception
     * @return boolean
     */
    public function clearCache( $offset=0 )
    {
        $this->_requester->open();

        $ret = $this->_requester->post($this->_getUrl('_cache/clear', $offset), NULL);
        $data = json_decode($ret, TRUE);
        if( isset($data['error']) ) throw new Exception(sprintf(__METHOD__ . ' (%s) %s', $data['status'], $data['error']));
        else $ret = TRUE;

        $this->_requester->close();

        return $ret;
    }

    /**
     * optimize
     * 
     * @access public
     * @param  array $options [Optional]
     * @param  interger $offset [Optional] 
     * @throws Exception
     * @return boolean
     */
    public function optimize( array $options=[], $offset=0 )
    {
        // TODO optimize

        return FALSE;
    }

    /**
     * setRefreshInterval
     * 
     * @access public
     * @param  string $interval
     * @param  interger $offset [Optional]
     * @throws Exception
     * @return Ambigous <boolean, mixed>
     */
    public function setRefreshInterval( $interval, $offset=0 )
    {
        $this->_requester->open();
        $this->_requester->setOptions([CURLOPT_TIMEOUT => 180]);

        $ret = $this->_requester->put($this->_getUrl('_settings', $offset), '{"index":{"refresh_interval":"'.$interval.'"}}');
        $data = json_decode($ret, TRUE);
        if( isset($data['error']) ) throw new Exception(sprintf(__METHOD__ . ' (%s) %s', $data['status'], $data['error']));
        else $ret = TRUE;

        $this->_requester->close();

        return $ret;
    }

    /**
     * setMergeFactor
     * 
     * @access public
     * @param  string $factor
     * @param  interger $offset [Optional]
     * @throws Exception
     * @return Ambigous <boolean, mixed>
     */
    public function setMergeFactor( $factor, $offset=0 )
    {
        $this->_requester->open();

        $ret = $this->_requester->put($this->_getUrl('_settings', $offset), '{"index":{"merge.policy.merge_factor":"'.$factor.'"}}');
        $data = json_decode($ret, TRUE);
        if( isset($data['error']) ) throw new Exception(sprintf(__METHOD__ . ' (%s) %s', $data['status'], $data['error']));
        else $ret = TRUE;

        $this->_requester->close();

        return $ret;
    }

    /**
     * _getUrl
     * 
     * @access private
     * @param  string $path
     * @param  integer $offset
     * @return string
     */
    private function _getUrl( $path, $offset )
    {
        return $this->_esDsn->getHost($offset) . '/' . $this->_esDsn->getIndex() . '/' . $path;
    }
}

<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4: */
/**
 * Elasticsearch Indexer - Sink
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 * PHP 5.4 Later
 * 
 * @final
 * @category   lib
 * @package    ElasticsearchIndex
 * @author     Junyong Park
 * @copyright  2013
 * @version    SVN: $Id:$
 */

require_once('ElasticsearchIndex_Helper.php');


final class ElasticsearchIndex_Sink
{
    /**
     * _socket
     * 
     * @access private
     * @var    array
     */
    private $_socket = [];

    /**
     * _taskType
     * 
     * @access private
     * @var    string
     */
    private $_taskType = NULL;

    /**
     * _workers
     * 
     * @access private
     * @var    integer
     */
    private $_workers = NULL;


    /**
     * Construct
     * 
     * @access public
     * @param  integer $workers
     * @param  string $taskType [Optional]
     * @throws UnexpectedValueException
     */
    public function __construct( $workers, $taskType=NULL )
    {
        // check parameter
        if( !is_int($workers) ) throw new UnexpectedValueException('invalid workers format');
        if( !is_null($taskType) && !is_scalar($taskType) ) throw new UnexpectedValueException('invalid task_type format');

        // set parameter
        $this->_workers = $workers;
        $this->_taskType = $taskType;
    }

    /**
     * open
     * 
     * @access public
     * @param  array $zmqDsn
     * @throws LogicException
     * @throws ZMQContextException
     * @throws ZMQSocketException
     * @return boolean
     */
    public function open( array $zmqDsn )
    {
        // create context instance
        $ctx = new ZMQContext();

        // create socket instance
        $this->_socket['sink'] = new ZMQSocket($ctx, ZMQ::SOCKET_PULL);
        $this->_socket['controller'] = new ZMQSocket($ctx, ZMQ::SOCKET_PUB);

        // bind
        $this->_socket['sink']->bind(@$zmqDsn['sink']);
        $this->_socket['controller']->bind(@$zmqDsn['controller']);

        return TRUE;
    }

    /**
     * run
     * 
     * @access public
     * @throws InvalidArgumentException
     * @throws ZMQSocketException
     * @return boolean
     */
    public function run()
    {
        // check socket
        if( !@$this->_socket['sink'] instanceof ZMQSocket ||
            !@$this->_socket['controller'] instanceof ZMQSocket )
            throw new InvalidArgumentException('Socket did not open'); 

        // initialize
        $msgCnt = ['start' => 0, 'end' => 0];
        $taskCnt = 0; 

        // Process tasks
        while( 1 )
        {
            // receive data
            $string = $this->_socket['sink']->recv();

            // start
            if( $string == ElasticsearchIndex_Helper::MSG_TASK_START )
            {
                $msgCnt['start']++;
                if( $msgCnt['start'] == $this->_workers ) $tstart = array_sum(explode(' ', microtime()));
            }
            // end
            elseif( $string == ElasticsearchIndex_Helper::MSG_TASK_END )
            {
                $msgCnt['end']++;
                if( $msgCnt['end'] == $this->_workers )
                {
                    $tend = array_sum(explode(' ', microtime()));
                    break;
                }
            }
            // error
            elseif( $string == ElasticsearchIndex_Helper::MSG_TASK_ERROR )
            {
                $this->_displayMsg('Error detected, aborting tasks');
                // send kill signal
                $this->_socket['controller']->send(ElasticsearchIndex_Helper::MSG_TASK_KILL);
            }
            // normal
            else
            {
                // increase task count
                ++$taskCnt;
            }
        }

        // set elapsed time
        if( $taskCnt ) $et = $tend - $tstart;
        else $et = 0;

        // display message
        $this->_displayMsg("Total($taskCnt) elapsed time: ".$et.' sec');

        // clear memory
        unset($msgCnt, $taskCnt, $string, $tend, $tstart, $et);

        return TRUE;
    }

    /**
     * close
     * 
     * @access public
     * @return boolean
     */
    public function close()
    {
        // close
        unset($this->_socket['sink'], $this->_socket['controller']);

        return TRUE;
    }

    /**
     * _disaplayMsg
     * 
     * @access private
     * @param  string $msg
     * @return void
     */
    private function _displayMsg( $msg )
    {
        ElasticsearchIndex_Helper::displayMsg($this->_taskType.'::sink::'.$msg);
    }
}

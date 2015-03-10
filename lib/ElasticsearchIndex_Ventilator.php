<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4: */
/**
 * Elasticsearch Indexer - Ventilator
 *
 *
 *
 *
 *
 *
 *
 * PHP 5.5 Later
 *
 * @final
 * @category   lib
 * @package    ElasticsearchIndex
 * @author     Junyong Park
 * @copyright  2013
 * @version    SVN: $Id:$
 */

require_once('ElasticsearchIndex_Helper.php');


final class ElasticsearchIndex_Ventilator
{
    /**
     * SENDING_SLEEP_AMOUNT
     * 
     * @access const
     * @var    integer
     */
    const SENDING_SLEEP_AMOUNT = 8000;

    /**
     * SENDING_SLEEP_TIME
     * 
     * @access const
     * @var    integer
     */
    const SENDING_SLEEP_TIME = 1;

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
        $this->_socket['ventilator'] = new ZMQSocket($ctx, ZMQ::SOCKET_PUSH);
        $this->_socket['controller'] = new ZMQSocket($ctx, ZMQ::SOCKET_SUB);

        // bind, connect
        $this->_socket['ventilator']->bind(@$zmqDsn['ventilator']);
        $this->_socket['controller']->connect(@$zmqDsn['controller']);

        // set socket option
//         $this->_socket['ventilator']->setSockOpt(ZMQ::SOCKOPT_LINGER, 0);
        $this->_socket['controller']->setSockOpt(ZMQ::SOCKOPT_SUBSCRIBE, NULL);

        return TRUE;
    }

    /**
     * run
     * 
     * @access public
     * @param  RiverInterface $river
     * @throws UnexpectedValueException
     * @throws InvalidArgumentException
     * @throws LogicException
     * @throws PDOException
     * @throws ZMQSocketException
     * @throws ZMQPollException
     * @return boolean
     */
    public function run( RiverInterface $river )
    {
        // check socket
        if( !@$this->_socket['ventilator'] instanceof ZMQSocket ||
            !@$this->_socket['controller'] instanceof ZMQSocket )
            throw new InvalidArgumentException('Socket did not open'); 

        // process error message from receiver and controller 
        $poll = new ZMQPoll();
        $poll->add($this->_socket['ventilator'], ZMQ::POLL_OUT);
        $poll->add($this->_socket['controller'], ZMQ::POLL_IN);
        $readable = $writable = [];

        // initialize
        $taskCnt = 0;
        $killed = FALSE;

        // open river
        $river->open();

        // send start signal to workers
        for( $i=0; $i<$this->_workers; $i++ ) $this->_socket['ventilator']->send(ElasticsearchIndex_Helper::MSG_TASK_START);
        $this->_displayMsg('Sending tasks to '.$this->_workers.' workers');

        // fetch and send task
        foreach( $river->fetch() as $row )
        {
//             $events = $poll->poll($readable, $writable, 500);
            $events = $poll->poll($readable, $writable);
            if( $events > 0 )
            {
                // out
                foreach( $writable as $socket )
                {
                    // to worker
                    if( $socket === $this->_socket['ventilator'] )
                    {
                        // increase task count
                        ++$taskCnt;

                        // send message
                        $this->_socket['ventilator']->send(serialize($row));

                        // sleep process
                        if( self::SENDING_SLEEP_AMOUNT && !($taskCnt % self::SENDING_SLEEP_AMOUNT) ) sleep(self::SENDING_SLEEP_TIME);
                    }
                }

                // in
                foreach( $readable as $socket )
                {
                    // from sink
                    if( $socket === $this->_socket['controller'] )
                    {
                        // receive data from sink
                        $string = $this->_socket['controller']->recv();

                        // kill(error)
                        if( $string == ElasticsearchIndex_Helper::MSG_TASK_KILL )
                        {
                            $killed = TRUE;

                            // abort to fetch
                            $river->abort();
                            $this->_displayMsg("Sending aborted by controller - Sent tasks($taskCnt)");

                            break 2;
                        }
                    }
                }
            }
        }

        // free
        unset($row);

        // send end signal to workers
        if( !$killed )
        {
            for( $i=0; $i<$this->_workers; $i++ ) $this->_socket['ventilator']->send(ElasticsearchIndex_Helper::MSG_TASK_END);
            $this->_displayMsg("Sent tasks($taskCnt)");
        }

        // close river
        $river->close();

        return !$killed;
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
        unset($this->_socket['ventilator'], $this->_socket['controller']);

        return TRUE;
    }

    /**
     * _displayMsg
     * 
     * @access private
     * @param  string $msg
     * @return void
     */
    private function _displayMsg( $msg )
    {
        ElasticsearchIndex_Helper::displayMsg(posix_getpid().'('.posix_getpid().') '.$this->_taskType.'::ventilator::'.$msg, FALSE);
    }
}

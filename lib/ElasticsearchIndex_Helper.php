<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4: */
/**
 * Elasticsearch Indexer - Helper
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 * PHP 5.3 Later
 * 
 * @category   lib
 * @package    ElasticsearchIndex
 * @author     Junyong Park
 * @copyright  2013
 * @version    SVN: $Id:$
 */


class ElasticsearchIndex_Helper
{
    /**
     * MSG_TASK_START
     * 
     * @access const
     * @var    string
     */
    const MSG_TASK_START = '--START--';

    /**
     * MSG_TASK_END
     * 
     * @access const
     * @var    string
     */
    const MSG_TASK_END = '--END--';

    /**
     * MSG_TASK_ERROR
     * 
     * @access const
     * @var    string
     */
    const MSG_TASK_ERROR = '--ERROR--';

    /**
     * MSG_TASK_KILL
     * 
     * @access const
     * @var    string
     */
    const MSG_TASK_KILL = '--KILL--';


    /**
     * writePid2Lock
     * 
     * @static
     * @access public
     * @param  string $lock
     * @param  integer $pid
     * @return boolean
     */
    public static function writePid2Lock( $lock, $pid )
    {
        if( ($fp = @fopen($lock, 'a')) === FALSE ) return FALSE;
        fwrite($fp, $pid.PHP_EOL);
        fclose($fp);
    
        return TRUE;
    }
    
    /**
     * checkProcState
     * 
     * @static
     * @access public
     * @param  string $lock
     * @param  integer $workers
     * @return boolean
     */
    public static function checkProcState( $lock, $workers )
    {
        if( ($fp = @fopen($lock, 'r')) === FALSE ) return FALSE;
    
        $cntProc = 0;
    
        while( !feof($fp) )
        {
            $cPid = preg_replace('/\s/', '', fgets($fp));
            if( !strlen($cPid) ) continue;
    
            exec("ps -p $cPid", $cPidState);
            if( count($cPidState) < 2 ) break;
    
            unset($cPidState);
            ++$cntProc;
        }
    
        fclose($fp);
    
        return ($cntProc == $workers + 1) ? TRUE : FALSE;
    }
    
    /**
     * killChildProcs
     * 
     * @static
     * @access public
     * @param  string $lock
     * @return boolean
     */
    public static function killChildProcs( $lock )
    {
        if( ($fp = @fopen($lock, 'r')) === FALSE ) return FALSE;

        while( !feof($fp) )
        {
            $cPid = preg_replace('/\s/', '', fgets($fp));
            if( strlen($cPid) ) posix_kill($cPid, 9);
        }

        fclose($fp);
    
        return TRUE;
    }

    /**
     * displayMsg
     * 
     * @static
     * @access public
     * @param  string $msg
     * @param  boolean $pidFlag [Optional]
     * @return void
     */
    public static function displayMsg( $msg, $pidFlag=TRUE )
    {
        if( $pidFlag ) echo date('[Y-m-d H:i:s]').' '.posix_getpid().'('.posix_getppid().') '.$msg.PHP_EOL;
        else echo date('[Y-m-d H:i:s]').' '.$msg.PHP_EOL;
    }

    /**
     * sendMail
     * 
     * @static
     * @access public
     * @param  array $headers
     * @param  string $subject
     * @param  string $body
     * @param  string $eol [Optional]
     * @param  string $charset [Optional]
     * @return boolean
     */
    public static function sendMail( array $headers, $subject, $body, $eol="\r\n", $charset='ISO-2022-JP' )
    {
        // check headers
        if( !isset($headers['to']) || !isset($headers['from']) ) return FALSE;

        // subject
        $subject = mb_encode_mimeheader(mb_convert_encoding($subject, $charset));

        // header
        $header  = 'MIME-Version: 1.0' . $eol;
        $header .= "From: {$headers['from']}" . $eol;
        $header .= "Content-Type: text/plain; charset={$charset}" . $eol;
        $header .= 'Content-Transfer-Encoding: 7bit' . $eol;

        // body
        $body = mb_convert_encoding($body, $charset);

        // param
//         $param = "-f{$headers['from']}";
        $param = NULL;

        // send
        return mail($headers['to'], $subject, $body, $header, $param);
    }
}

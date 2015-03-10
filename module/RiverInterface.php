<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4: */
/**
 * RiverInterface
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 * PHP 5.3 Later
 * 
 * @abstract
 * @category   module
 * @package    river
 * @author     Junyong Park
 * @copyright  2014
 * @version    SVN: $Id:$
 */


interface RiverInterface
{
    /**
     * open
     * 
     * @access public
     * @return boolean
     */
    public function open();

    /**
     * fetch
     * 
     * @access public
     * @return array
     */
    public function fetch();

    /**
     * abort
     * 
     * @access public
     * @return boolean
     */
    public function abort();

    /**
     * options
     * 
     * @access public
     * @return boolean
     */
    public function options( array $option );

    /**
     * close
     * 
     * @access public
     * @return boolean
     */
    public function close();
}

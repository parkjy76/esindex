<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4: */
/**
 * BaseMapper
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
 * @package    mapper
 * @author     Junyong Park
 * @copyright  2014
 * @version    SVN: $Id:$
 */


Abstract class BaseMapper
{
    /**
     * _instance
     * 
     * @static
     * @access protected
     * @var    object
     */
    protected static $_instance = NULL;


    /**
     * Constructor
     * 
     * @access protected
     * @return void
     */
    protected function __construct() {}

    /**
     * __clone
     * 
     * @final
     * @access public
     * @return void
     * @throws BadMethodCallException
     */
    final public function __clone()
    {
        throw new BadMethodCallException("Clone is not allowed");
    }

    /**
     * getInstance
     * 
     * @static
     * @access public
     * @return object
     */
    public static function getInstance()
    {
        if( !static::$_instance instanceof BaseMapper ) static::$_instance = new static();

        return static::$_instance;
    }

    /**
     * convert
     * 
     * @abstract
     * @access public
     * @param  array $lineArr
     * @return string
     */
    abstract public function convert( array $var );
}

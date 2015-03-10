<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4: */
/**
 * Elasticsearch Indexer - Loader
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 * PHP 5.3 Later
 * 
 * 
 * @category   lib
 * @package    ElasticsearchIndex
 * @author     Junyong Park
 * @copyright  2014
 * @version    SVN: $Id:$
 */


class ElasticsearchIndex_Loader
{
    /**
     * ES_INI_FILE
     * 
     * @access const
     * @var    string
     */
    const ES_INI_FILE = 'es.ini';

    /**
     * ZMQ_INI_FILE
     * 
     * @access const
     * @var    string
     */
    const ZMQ_INI_FILE = 'zmq.ini';

    /**
     * MAPPER_INI_FILE
     * 
     * @access const
     * @var    string
     */
    const MAPPER_INI_FILE = 'mapper.ini';

    /**
     * RIVER_INI_FILE
     * 
     * @access const
     * @var    string
     */
    const RIVER_INI_FILE = 'river.ini';


    /**
     * parseIni
     * 
     * @static
     * @access public
     * @param  string $iniFile
     * @return boolean|array
     */
    public static function parseIni( $iniFile )
    {
        return @parse_ini_file(ETC_DIR . $iniFile, TRUE);
    }

    /**
     * esDsn
     * 
     * @static
     * @access public
     * @param  string $taskType
     * @return boolean|array
     */
    public static function esDsn( $taskType )
    {
        static $var = NULL;

        if( !is_array($var) )
        {
            // parse ini
            if( ($var = static::parseIni(self::ES_INI_FILE)) === FALSE )
                return FALSE;
        }

        if( !isset($var[$taskType]) ) return FALSE;

        return $var[$taskType];
    }

    /**
     * zmqDsn
     * 
     * @static
     * @access public
     * @param  string $taskType
     * @return boolean|array
     */
    public static function zmqDsn( $taskType )
    {
        static $var = NULL;

        if( !is_array($var) )
        {
            // parse ini
            if( ($var = static::parseIni(self::ZMQ_INI_FILE)) === FALSE )
                return FALSE;
        }

        if( !isset($var[$taskType]) ) return FALSE;

        return $var[$taskType];
    }

    /**
     * mapperDsn
     * 
     * @static
     * @access public
     * @param  string $taskType
     * @return boolean|array
     */
    public static function mapperDsn( $taskType )
    {
        static $var = NULL;

        if( !is_array($var) )
        {
            // parse ini
            if( ($var = static::parseIni(self::MAPPER_INI_FILE)) === FALSE )
                return FALSE;
        }

        if( !isset($var[$taskType]) ) return FALSE;

        return $var[$taskType];
    }

    /**
     * riverDsn
     * 
     * @static
     * @access public
     * @param  string $taskType
     * @return boolean|array
     */
    public static function riverDsn( $taskType )
    {
        static $var = NULL;

        if( !is_array($var) )
        {
            // parse ini
            if( ($var = static::parseIni(self::RIVER_INI_FILE)) === FALSE )
                return FALSE;
        }

        if( !isset($var[$taskType]) ) return FALSE;

        return $var[$taskType];
    }

    /**
     * schema
     * 
     * @static
     * @access public
     * @param  string $file
     * @return mixed
     */
    public static function schema( $file )
    {
        if( !strlen($file) ) return FALSE;

        return @file_get_contents(ETC_DIR . $file);
    }

    /**
     * query
     * 
     * @static
     * @access public
     * @param  string $file
     * @return mixed
     */
    public static function query( $file )
    {
        if( !strlen($file) ) return FALSE;

        return @file_get_contents(ETC_DIR . 'query/' . $file);
    }

    /**
     * mapper
     * 
     * @static
     * @access public
     * @param  array $dsn
     * @return BaseMapper
     * @throws UnexpectedValueException
     */
    public static function mapper( array $dsn )
    {
        if( !isset($dsn['name']) || !file_exists(MOD_DIR . "mapper/${dsn['name']}.php") )
            throw new UnexpectedValueException('Fail to load mapper');

        require_once(MOD_DIR . "mapper/${dsn['name']}.php");
        $mapperClass = "\\${dsn['name']}\\Mapper";

        // create mapper instance and return it
        return $mapperClass::getInstance();
    }

    /**
     * river
     * 
     * @static
     * @access public
     * @param  array $dsn
     * @param  string $query
     * @return RiverInterface
     * @throws UnexpectedValueException
     */
    public static function river( array $dsn, $query )
    {
        if( !isset($dsn['driver']) || !file_exists(MOD_DIR . "river/{$dsn['driver']}.php") )
            throw new UnexpectedValueException('Fail to load river');

        require_once(MOD_DIR . "river/{$dsn['driver']}.php");
        $riverClass = '\\'.$dsn['driver'].'\\River';

        return new $riverClass($dsn, $query);
    }
}

<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4: */
/**
 * Mapper - test
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
 * @category   module
 * @package    mapper
 * @author     Junyong Park
 * @copyright  2014
 * @version    SVN: $Id:$
 */

namespace test;

require_once(MOD_DIR . 'BaseMapper.php');


class Mapper extends \BaseMapper
{
    /**
     * convert
     * 
     * @access public
     * @param  array $lineArr
     * @return array
     * @see BaseMapper::convert()
     */
    public function convert( array $lineArr )
    {
        return $this->_item_type($lineArr);
    }

    /**
     * _item_type
     * 
     * @access private
     * @param  array $row
     * @return array
     */
    private function _item_type( array $row )
    {
        // document id 
        $docId = [
            '_type' => 'item',
            '_id' => $row[0],
        ];

        // index information
        $index = ['index' => $docId];

        // data
        $data = [
            'name' => $row[1],
            'price' => $row[2],
        ];

        // return json data
        return [implode($docId) => json_encode($index) . PHP_EOL . json_encode($data, JSON_UNESCAPED_UNICODE)];
    }
}

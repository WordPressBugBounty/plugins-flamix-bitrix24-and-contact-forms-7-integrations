<?php

namespace Flamix\Plugin\General;

class Status
{
    public static function key(string $key): string
    {
        $key = Helpers::slug($key, '_');
        return strtoupper(Helpers::limit($key, 60, ''));
    }
}

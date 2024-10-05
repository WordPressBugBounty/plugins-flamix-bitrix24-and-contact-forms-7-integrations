<?php

if (!function_exists('tap')) {
    /**
     * Tap, tap, tap....
     *
     * @param $value
     * @param callable $callback
     * @return mixed
     */
    function tap($value, ?callable $callback = null)
    {
        if (is_null($callback)) {
            return new \Flamix\Plugin\General\HigherOrderTapProxy($value);
        }

        $callback($value);
        
        return $value;
    }
}

if (!function_exists('flamix_log')) {
    /**
     * flamix_log('Hello', [1,2,3], 'commerceml');
     *
     * @param string|bool|int $message Log message
     * @param array $context Data
     * @param string|null $chanel Folder
     * @return \Monolog\Logger
     * @throws Exception
     */
    function flamix_log($message, array $context = [], ?string $chanel = null): \Monolog\Logger
    {
        return \Flamix\Plugin\General\Helpers::log($message, $context, $chanel);
    }
}

if (! function_exists('throw_if')) {
    /**
     * Throw the given exception if the given condition is true.
     *
     * @param  mixed  $condition
     * @param  \Throwable|string  $exception
     * @param  mixed  ...$parameters
     * @return mixed
     *
     * @throws \Throwable
     */
    function throw_if($condition, $exception = 'RuntimeException', ...$parameters)
    {
        if ($condition) {
            if (is_string($exception) && class_exists($exception)) {
                $exception = new $exception(...$parameters);
            }

            throw is_string($exception) ? new RuntimeException($exception) : $exception;
        }

        return $condition;
    }
}

if (! function_exists('throw_unless')) {
    /**
     * Throw the given exception unless the given condition is true.
     *
     * @param  mixed  $condition
     * @param  \Throwable|string  $exception
     * @param  mixed  ...$parameters
     * @return mixed
     *
     * @throws \Throwable
     */
    function throw_unless($condition, $exception = 'RuntimeException', ...$parameters)
    {
        throw_if(! $condition, $exception, ...$parameters);

        return $condition;
    }
}
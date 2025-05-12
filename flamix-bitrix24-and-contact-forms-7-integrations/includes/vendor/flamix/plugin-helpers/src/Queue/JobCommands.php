<?php

namespace Flamix\Plugin\Queue;

class JobCommands
{
    /**
     * Is table to store Order Jobs exist?
     *
     * @return bool
     */
    public function isTableExist(): bool
    {
        if (empty($this->query($this->sqlClosure()->describeTable()))) {
            return false;
        }

        return true;
    }

    /**
     * Create table to store Order Jobs.
     *
     * @return bool
     */
    public function createQueueTableIfNotExist(): bool
    {
        if ($this->isTableExist()) {
            return true;
        }

        // Create table
        $this->query($this->sqlClosure()->createTable());

        return $this->isTableExist();
    }

    /**
     * Update Job.
     *
     * @param  array  $fields
     * @param  array  $where
     * @return mixed
     */
    public function update(array $fields, array $where)
    {
        return $this->query($this->sqlClosure()->update($fields, $where));
    }

    /**
     * Only add new order to Queue.
     *
     * @param  int  $order_id
     * @return int
     */
    public function create(int $order_id): int
    {
        return $this->query($this->sqlClosure()->insert($order_id, 'NEW'))['0']->id ?? 0;
    }

    /**
     * Update if order_id exist.
     * Create if order_id not exist.
     *
     * @param $order_id
     * @param  array  $data
     * @return int
     */
    public function createOrUpdate($order_id, array $data): int
    {
        $id = $this->query($this->sqlClosure()->select(['order_id' => $order_id], ['id'], 1))['0']->id ?? 0;

        // Insert: if we can't find
        if (!$id) {
            return $this->create($order_id);
        }

        // Update when we found id
        $this->update($data, ['id' => $id]);
        return $id;
    }

    /**
     * Lock all orders.
     *
     * @param  array  $orders_id
     * @return int How many orders locked
     */
    public function lock(array $orders_id): int
    {
        $this->query($this->sqlClosure()->updateWhereIn(['reserved_at' => 'now', 'updated_at' => 'now'], 'order_id',
            $orders_id));
        return count($orders_id);
    }

    /**
     * Check if order is sent successfully.
     *
     * @param  int  $order_id
     * @return bool
     */
    public function isSentSuccessfully(int $order_id): bool
    {
        $sql = $this->sqlClosure()->select(['order_id' => $order_id], ['order_id', 'order_job_status'], 1);
        $status = $this->query($sql)['0']->order_job_status ?? 'NOT_FOUND';
        return $status === $this->success;
    }
}
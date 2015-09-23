<?php

class AzureStorageUtil
{
	protected $accountKey;
	protected $account;
	protected $connectionString;
	
	public function __construct($account, $accountKey)
	{
		$this->account = $account;
		$this->accountKey = $accountKey;
		$this->connectionString = sprintf('DefaultEndpointsProtocol=%s;AccountName=%s;AccountKey=%s', 'https', $account, $accountKey);
	}

	public function getConnectionString()
	{
		return $this->connectionString;
	}
}

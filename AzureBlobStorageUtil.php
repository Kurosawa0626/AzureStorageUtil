<?php

use WindowsAzure\Common\ServicesBuilder;
use WindowsAzure\Blob\Models\ListBlobsOptions;
use WindowsAzure\Blob\Models\PublicAccessType;
use WindowsAzure\Blob\Models\CreateContainerOptions;

require_once __DIR__.'/AzureStorageUtil.php';
class AzureBlobStorageUtil extends AzureStorageUtil
{
	private $blobRestProxy;
	public function getBlobRestProxy()
	{
		return $this->blobRestProxy;
	}

	public function __construct($account, $accountKey)
	{
		parent::__construct($account, $accountKey);
		$this->blobRestProxy = ServicesBuilder::getInstance()->createBlobService($this->getConnectionString());
	}

	/**
	 * コンテナーを作成する
	 * 
	 * IMPORTANT:
	 * コンテナーの名前は、常に小文字にする必要があります。
	 * コンテナー名に大文字が含まれている場合や、コンテナーの名前付け規則の他の違反がある場合、400 エラー (無効な要求) が発生することがあります。
	 * コンテナーの名前付け規則については、https://msdn.microsoft.com/library/azure/dd135715.aspxをご覧ください。
	 * 
	 * @param type $containerName
	 * @param type $publicAccess
	 * @param type $metadata
	 * @throws Exception
	 */
	public function createBlobContainer($containerName, $publicAccess = PublicAccessType::NONE, $metadata = null)
	{
		$createContainerOptions = new CreateContainerOptions();
		$createContainerOptions->setPublicAccess($publicAccess);
		if($metadata)
		{
			$createContainerOptions->setMetadata($metadata);
		}

		try
		{
			$this->blobRestProxy->createContainer($containerName, $createContainerOptions);
		}
		catch(Exception $e)
		{
			throw new Exception($e->getMessage(), $e->getCode());
		}
	}

	/**
	 * コンテナーが存在するか
	 * @param type $containerName
	 */
	public function existsContainer($containerName)
	{
		try
		{
			$containerList = $this->blobRestProxy->listContainers();
			foreach($containerList->getContainers() as $container)
			{
				if($container->getName() === $containerName)
				{
					return TRUE;
				}
			}
		}
		catch(Exception $e)
		{
			throw new Exception($e->getMessage(), $e->getCode());
		}

		return FALSE;
	}
	
	/**
	 * コンテナーに BLOB をアップロードする。
	 * BLOB が存在しない場合は作成され、存在する場合は上書きされます。
	 * 
	 * @param type $containerName
	 * @param type $blob
	 * @param type $content
	 * @throws Exception
	 */
	public function createBlob($containerName, $blob, $content)
	{
		try 
		{
			if(!self::existsContainer($containerName))
			{
				self::createBlobContainer($containerName);
			}
			$this->blobRestProxy->createBlockBlob($containerName, $blob, $content);
		}
		catch(Exception $e)
		{
			throw new Exception($e->getFile()." on line ".$e->getLine(), $e->getCode());
		}
	}
	
	/**
	 * コンテナ名の一覧を取得する
	 * @return type
	 */
	public function getContainerNameList()
	{
		$containerNameList = array();
		$containerList = $this->blobRestProxy->listContainers();
		foreach($containerList->getContainers() as $container)
		{
			$containerNameList[] = $container->getName();
		}
		
		return $containerNameList;
	}

	/**
	 * Blobの取得
	 * @param type $container
	 * @param type $blob
	 * @throws Exception
	 */
	public function getBlob($container, $blob)
	{
		try
		{
			return $this->blobRestProxy->getBlob($container, $blob);
		}
		catch (Exception $e)
		{
			throw new Exception($e->getMessage(), $e->getCode());
		}
	}
	
	/**
	 * Blob名一覧の取得
	 * @param type $containerName
	 * @return type
	 * @throws Exception
	 */
	public function getBlobNameList($containerName, $prefix = null, $delimiter = null)
	{
		$options = new ListBlobsOptions();
		$blobNameList = array();

		while(true)
		{
			try
			{
				if($prefix)
				{
					$options->setPrefix($prefix);
				}
				if($delimiter)
				{
					$options->setDelimiter($delimiter);
				}
				$blobList = $this->blobRestProxy->listBlobs($containerName, $options);
			}
			catch(Exception $e)
			{
				throw new Exception($e->getMessage(), $e->getCode());
			}

			foreach($blobList->getBlobs() as $blob)
			{
				$blobNameList[] = $blob->getName();
			}
			
			$nextMarker = $blobList->getNextMarker();
			if (!$nextMarker || strlen($nextMarker) == 0) 
			{
				break;
			}
			$options->setMarker($nextMarker);
		};

		return $blobNameList;
	}

	/**
	 * BlobのPrefix一覧取得
	 * @param type $containerName
	 * @return type
	 * @throws Exception
	 */
	public function getBobPrefixes($containerName, $prefix = null, $delimiter = null)
	{
		$options = new ListBlobsOptions();
		$blobPrefixes = array();

		while(true)
		{
			try
			{
				if($prefix)
				{
					$options->setPrefix($prefix);
				}
				if($delimiter)
				{
					$options->setDelimiter($delimiter);
				}
				$blobList = $this->blobRestProxy->listBlobs($containerName, $options);
			}
			catch(Exception $e)
			{
				throw new Exception($e->getMessage(), $e->getCode());
			}

			foreach($blobList->getBlobPrefixes() as $blobPrefix)
			{
				$blobPrefixes[] = $blobPrefix->getName();
			}
			
			$nextMarker = $blobList->getNextMarker();
			if (!$nextMarker || strlen($nextMarker) == 0) 
			{
				break;
			}
			$options->setMarker($nextMarker);
		};

		return $blobPrefixes;
	}

	/**
	 * Blob削除
	 * @param type $container
	 * @param type $blob
	 * @throws Exception
	 */
	public function deleteBlob($container, $blob)
	{
		try
		{
			$this->blobRestProxy->deleteBlob($container, $blob);
		}
		catch(Exception $e)
		{
			throw new Exception($e->getMessage(), $e->getCode());
		}
	}
}

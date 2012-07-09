<?php

	loadlib("http");

	$GLOBALS['cfg']['elasticsearch_host'] = 'http://localhost';
	$GLOBALS['cfg']['elasticsearch_port'] = 9200;

	########################################################################

	function elasticsearch_index($index, $type, $data, $id=null){

		$index = urlencode($index);
		$type = urlencode($type);

		$endpoint = $GLOBALS['cfg']['elasticsearch_host'] . ":" . $GLOBALS['cfg']['elasticsearch_port'];
		$url = "{$endpoint}/{$index}/{$type}/";

		$args = json_encode($data);

		if (! $id){
			return http_post($url, $args);
		}

		$id = urlencode($id);
		$url .= "{$id}/";

		$headers = array();

		$more = array(
			'donotsend_transfer_encoding' => 1
		);

		return http_put($url, $args, $headers, $more);
	}

	########################################################################

	# http://www.elasticsearch.org/guide/reference/api/bulk.html

	function elasticsearch_bulk_index($index, $type, $data){

		$req = array();

		$header = array(
			"index" => array(
				"_index" => $index,
				"_type" => $type,
			)
		);

		# TO DO: ids...

		foreach ($data as $blob){
			$req[] = json_encode($header);
			$req[] = json_encode($blob, JSON_FORCE_OBJECT);
		}

		$req = implode("\n", $req);

		$index = urlencode($index);
		$type = urlencode($type);

		$endpoint = $GLOBALS['cfg']['elasticsearch_host'] . ":" . $GLOBALS['cfg']['elasticsearch_port'];
		$url = "{$endpoint}/{$index}/{$type}/_bulk";

		return http_post($url, $req);
	}

	########################################################################

	# http://www.elasticsearch.org/guide/reference/api/search/uri-request.html

	function elasticsearch_search_all($q, $more=array()){

		$_more = $more;

		$_more['per_page'] = 1;
		$_more['page'] = 1;

		$rsp = elasticsearch_search($q, $_more);

		if (! $rsp['ok']){
			return $rsp;
		}

		$_more['per_page'] = $rsp['pagination']['total_count'];
		return elasticsearch_search($q, $_more);
	}

	########################################################################

	function elasticsearch_search($q, $more=array()){

		$page = isset($more['page']) ? max(1, $more['page']) : 1;
		$per_page = isset($more['per_page']) ? max(1, $more['per_page']) : $GLOBALS['cfg']['pagination_per_page'];

		$from = ($page - 1) * $per_page;
		$size = $per_page;

		#

		$query = array();

		foreach ($q as $k => $v){
			$query[] = "{$k}:{$v}";
		}

		$params = array(
			'q' => implode(" AND ", $query),
			'from' => $from,
			'size' => $size,
		);

		$endpoint = $GLOBALS['cfg']['elasticsearch_host'] . ":" . $GLOBALS['cfg']['elasticsearch_port'];
		$url = "{$endpoint}/";

		if (isset($more['index'])){
			$index = urlencode($more['index']);
			$url .= "{$index}/";
		}

		if (isset($more['type'])){
			$type = urlencode($more['type']);
			$url .= "{$type}/";
		}

		$url .= "_search?" . http_build_query($params);
		$rsp = http_get($url);

		if (! $rsp['ok']){
			return $rsp;
		}

		$data = json_decode($rsp['body'], 'as hash');

		if (! $data){
			return not_okay("Failed to parse JSON");
		}

		# 		

		$total_count = $data['hits']['total'];
		$page_count = ceil($total_count / $per_page);
		$last_page_count = $total_count - (($page_count - 1) * $per_page);

		$rsp = array(
			'ok' => 1,
			'rows' => $data['hits']['hits'],
			'pagination' => array(
				'total_count' => $total_count,
				'page' => $page,
				'per_page' => $per_page,
				'page_count' => $page_count,
			)
		);

		return okay($rsp);
	}

	########################################################################
?>

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
		$req .= "\n";

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

		$defaults = array(
			'mode' => 'all',
		);

		$more = array_merge($defaults, $more);

		$page = isset($more['page']) ? max(1, $more['page']) : 1;
		$per_page = isset($more['per_page']) ? max(1, $more['per_page']) : $GLOBALS['cfg']['pagination_per_page'];

		$from = ($page - 1) * $per_page;
		$size = $per_page;

		#

		$query = array();

		foreach ($q as $k => $v){
			$query[] = "{$k}:{$v}";
		}

		$join = ($more['mode'] == 'or') ? "OR" : "ALL";

		$params = array(
			'q' => implode(" {$join} ", $query),
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


		# TO DO: put this someplace common (like not here or lib_db)

		if ($GLOBALS['cfg']['pagination_assign_smarty_variable']) {
			$GLOBALS['smarty']->assign('pagination', $rsp['pagination']);
			$GLOBALS['smarty']->register_function('pagination', 'smarty_function_pagination');
		}

		return okay($rsp);
	}

	########################################################################

	# This does *not* use a simple JSON-ified version of the ES query DSL
	# because 1) it runs ashore if PHP's JSON serialization when it comes to
	# to handling lists and hashes and 2) because it's just so long and
	# boring to type (20120709/straup)

	# http://www.elasticsearch.org/guide/reference/api/search/facets/index.html

	function elasticsearch_search_faceted($q, $more=array()){

		$defaults = array(
			'mode' => 'all',
		);

		$more = array_merge($defaults, $more);

		if (! isset($q['size'])){
			$q['size'] = 0;
		}

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

		$url .= "_search";

		# Are you ready? Because it's about to get ugly. Basically we're
		# going to build the big scary ES JSON blob and then we're going
		# to encode filters and facet fields separately so that we can
		# str_replace them back in to the already encoded JSON such that
		# are included as lists. Because your life wasn't already awful
		# enough... (20120709/straup)

		$query_filters = array();
		$facet_fields = array();

		$query_keys = array_keys($q['query']);
		$query_values = array_values($q['query']);

		$count_terms = count($query_keys);		

		$first_query = array(
			"term" => array( $query_keys[0] => $query_values[0] )
		);

		if ($count_terms == 1){
			$q['query'] = $first_query;
		}

		else {

			# http://www.elasticsearch.org/guide/reference/query-dsl/or-filter.html
			# http://www.elasticsearch.org/guide/reference/query-dsl/and-filter.html

			for ($i=1; $i < $count_terms; $i++){
				$filter = array( "term" => array( $query_keys[$i] => $query_values[$i] ));
				$query_filters[] = json_encode($filter, JSON_FORCE_OBJECT);		
			}		

			$join = ($more['mode'] == 'or') ? "or" : "and";

			# I want to cry: https://gist.github.com/732957

			$q['query'] = array('filtered' => array(
				'query' => $first_query,
				'filter' => array( $join => array("filters" => "__FILTERS__" )),
			));
		}

		
		foreach ($q['facets'] as $k){
			$facet_fields[] = "\"{$k}\"";
		}

		$q['facets'] = array(
			"results" => array(
				"terms" => array("field" => "__FACETS__"),
			),
		);

		$body = json_encode($q, JSON_FORCE_OBJECT);

		# SAD FACE...

		if ($count_terms > 1){
			$str_filters = "[" . implode(",", $query_filters) . "]";
			$body = str_replace("\"__FILTERS__\"", $str_filters, $body);
		}

		$str_facets = "[" . implode(",", $facet_fields) . "]";
		$body = str_replace("\"__FACETS__\"", $str_facets, $body);

		$rsp = http_post($url, $body);

		if (! $rsp['ok']){
			return $rsp;
		}

		$data = json_decode($rsp['body'], 'as hash');

		if (! $data){
			return not_okay("Failed to parse JSON");
		}

		return okay(array(
			'facets' => $data['facets']
		));
	}

	########################################################################

?>

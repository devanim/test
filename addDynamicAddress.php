<!DOCTYPE html>
<html>

<head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <title>getAmBrands()</title>
</head>

<body>
    <?php

    // Constant values:
    define('TECDOC_MANDATOR', 20656);
    define('SERVICE_URL', 'https://webservice.tecalliance.services/pegasus-3-0/services/TecdocToCatDLB.jsonEndpoint');

    // Printout any error:
    error_reporting(E_ALL | E_NOTICE); // to see all Errors
    
    // Setup HTTP context with communication header:
    function getContext($data, $optional_headers)
    {
        $params = array(
            'http' => array(
                'method' => 'POST',
                'content' => $data
            )
        );

        if ($optional_headers !== null) {
            $params['http']['header'] = $optional_headers;
        }

        return stream_context_create($params);
    }

    // Create request with function name and its parameters:
    function createRequest($functionName, $requestParams)
    {
        return array(
            $functionName => $requestParams
        );
    }

    // Serializing request, calling JSON endpoint & deserializing response:
    function callJSON($function, $request, $optional_headers = null)
    {
        $jsonRequest = json_encode($request);

        $ctx = getContext($jsonRequest, $optional_headers);
        $fp = @fopen(SERVICE_URL, 'rb', false, $ctx);
        if (!$fp) {
            throw new Exception("Problem with $url, $php_errormsg");
        }

        $jsonResponse = @stream_get_contents($fp);
        if ($jsonResponse === false) {
            throw new Exception("Problem reading data from $url, $php_errormsg");
        }

        $response = json_decode($jsonResponse);
        return $response;
    }

    // TestRequestParameters:
    $function = 'getAmBrands';

    $params = array(
        'address' => '94.130.219.93',
        'lang' => 'ro',
        'provider' => TECDOC_MANDATOR
    );

    $request = createRequest($function, $params);

    echo "<h1>Calling function " . $function . ":</h1>";

    echo "<br><b>REQUEST:</b><br>";
    echo "<pre>";
    print_r($request);

    $result = callJSON($function, $request);

    echo "<br><b>RESPONSE:</b><br>";
    echo "<pre>";
    print_r($result);

    ?>
</body>

</html>
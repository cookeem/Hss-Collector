/**
 * Created by cookeem on 16/6/3.
 */
app.controller('resultAppCtl', function($scope, $rootScope, $timeout, $http, $routeParams) {
    $scope.took = 0;
    $scope.resultData = [];
    $scope.rscount = 0;
    $scope.page = 1;
    $scope.count = 10;
    $scope.pages = 0;
    $scope.range = [];
    $scope.showpages = 9;

    $scope.neType = $routeParams.querystring;
    if ($scope.neType == "huawei") {
        //所有显示的字段

        $scope.allFields = [
            "SERIAL_NO",
            "HLR_INDEX",
            "OPERATOR_NAME",
            "OPERATION_TIME",
            "MML_COMMAND",
            "CMDRESULT",
            "BATCH_TASK_ID",
            "COMMAND_NO",
            "MSG_TYPE",
            "IMSI_NO",
            "MSISDN_NO",
            "ERRORCODE"
        ];

        $scope.searchTerm = {
            "SERIAL_NO": "",
            "HLR_INDEX": "",
            "OPERATOR_NAME": "",
            //"OPERATION_TIME": "",
            "MML_COMMAND": "",
            "CMDRESULT": "",
            "BATCH_TASK_ID": "",
            "COMMAND_NO": "",
            "MSG_TYPE": "",
            "IMSI_NO": "",
            "MSISDN_NO": "",
            "ERRORCODE": ""
        };

        $scope.formData = {
            searchType: 1,
            showFields: $scope.allFields,
            count: "",
            descSort: "",
            fromStartTime: "",
            toStartTime: ""
        };
    } else {
        //所有显示的字段
        $scope.allFields = [
            "FullRequest",
            "FullResponse",
            "StartTime",
            "Target",
            "Status",
            "Operation",
            "Protocol",
            "LogType",
            "ResponseCode",
            "ExecuteTime",
            "TransactionId",
            "SubLogId",
            "Hostname",
            "User",
            "RootLogId",
            "Instance"
        ];

        $scope.searchTerm = {
            "FullRequest" : "ISDN",
            "FullResponse" : "",
            //"StartTime" : "",
            "Target" : "",
            "Status" : "",
            "Operation" : "",
            "Protocol" : "",
            "LogType" : "",
            "ResponseCode" : "",
            "ExecuteTime" : "",
            "TransactionId" : "",
            "SubLogId" : "",
            "Hostname" : "",
            "User" : "",
            "RootLogId" : "",
            "Instance" : ""
        };

        $scope.formData = {
            searchType: 0,
            showFields: $scope.allFields,
            count: "",
            descSort: "",
            fromStartTime: "",
            toStartTime: ""
        };
    }

    $scope.submitSearch = function() {
        var termArray = $.map($scope.searchTerm, function(v, k) {
            return [{ field: k, term: v}];
        });
        var termFields = JSON.stringify(termArray);
        if ($scope.formData.count != "") {
            $scope.count = $scope.formData.count;
        }
        $scope.searchData = {
            searchType: $scope.formData.searchType,
            fields: $scope.formData.showFields.join(),
            page: $scope.page,
            count: $scope.formData.count,
            descSort: $scope.formData.descSort,
            fromStartTime: $scope.formData.fromStartTime,
            toStartTime: $scope.formData.toStartTime,
            termFields: termFields
        };

        $rootScope.isLoading = true;
        $http({
            method  : 'POST',
            url     : '/json/query',
            data    : $.param($scope.searchData),
            headers : { 'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8' }
        }).then(function successCallback(response) {
            $rootScope.errmsg = response.data.errmsg;
            //假如有错误,跳转到错误显示页面
            if ($rootScope.errmsg != "") {
                $rootScope.errurl = "javascript:history.back()";
                $rootScope.errbtn = "返 回";
                window.location.href = "index.html#!/error";
            }
            if (response.data) {
                $scope.resultData = response.data.data.map(function(obj) {
                    return $.map(obj, function(v, k) {
                        var str = html_encode(v).replace(/##begin##/g, '<span class="chip red white-text">').replace(/##end##/g,'</span>');
                        return [{ k: k, v: str}];
                    });
                });
            }
            if (response.data.rscount) {
                $scope.rscount = response.data.rscount;
            }
            if (response.data.took) {
                $scope.took = response.data.took;
            }
            $scope.pages = Math.ceil($scope.rscount / $scope.count);
            if ($scope.pages > 500) {
                $scope.pages = 500;
            }
            var minpage = 0;
            var maxpage = 0;
            var slide = Math.ceil(($scope.showpages-1)/2);
            if ($scope.pages <= $scope.showpages) {
                minpage = 1;
                maxpage = $scope.pages;
            } else {
                if ($scope.page - slide < 1) {
                    minpage = 1;
                    if ($scope.showpages > $scope.pages) {
                        maxpage = $scope.pages;
                    } else {
                        maxpage = $scope.showpages;
                    }
                } else {
                    if ($scope.page + slide > $scope.pages) {
                        minpage = $scope.pages - $scope.showpages + 1;
                        maxpage = $scope.pages;
                    } else {
                        minpage = $scope.page - slide;
                        maxpage = $scope.page + slide;
                    }
                }
            }
            var range = [];
            for(var i=minpage;i<=maxpage;i++) {
                range.push(i);
            }
            $scope.range = range;
            $rootScope.isLoading = false;
        }, function errorCallback(response) {
            console.info("error:" + response.data);
            $rootScope.isLoading = false;
        });
    };

    $scope.pageChange = function(targetPage) {
        $scope.page = targetPage;
        $scope.submitSearch();
    };

    $scope.pressSearch = function() {
        $scope.page = 1;
        $scope.submitSearch();
    };

    $timeout(function() {
        $('select').material_select();
    }, 100);

    $scope.submitSearch();
});
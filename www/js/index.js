/**
 * Created by cookeem on 16/6/2.
 */
var app = angular.module('app', ['ngRoute', 'ngAnimate']);

app.run(function($timeout) {
    //初始化materialize select
    $timeout(function() {
        $('select').material_select();
    }, 100);
});

app.config(function($routeProvider, $locationProvider) {
    $routeProvider
        .when('/result', {
            templateUrl: 'result.html',
            controller: 'contentCtl',
            animation: 'animation-slideleft'
        })
        .when('/error', {
            templateUrl: 'error.html',
            controller: 'contentCtl',
            animation: 'animation-slideleft'
        })
        .otherwise({redirectTo: '/error'});
    //使用#!作为路由前缀
    $locationProvider.html5Mode(false).hashPrefix('!');
});

app.controller('headerCtl', function($rootScope) {

});

app.controller('contentCtl', function($rootScope, $scope, $route, $routeParams) {
    $rootScope.params = $routeParams;

    $rootScope.$on('$routeChangeStart', function(event, currRoute, prevRoute){
        $rootScope.animation = currRoute.animation;
        $('html, body').animate({scrollTop:0}, 0);
        $rootScope.isLoading = true;
    });
    $rootScope.$on('$routeChangeSuccess', function() {
        $rootScope.isLoading = false;
    });
});

app.filter('trustHtml', function ($sce) {
    return function (input) {
        return $sce.trustAsHtml(input);
    }
});

function html_encode(str) {
    var s = "";
    if (str.length == 0) return "";
    s = str.replace(/&/g, "&gt;");
    s = s.replace(/</g, "&lt;");
    s = s.replace(/>/g, "&gt;");
    s = s.replace(/ /g, "&nbsp;");
    s = s.replace(/\'/g, "&#39;");
    s = s.replace(/\"/g, "&quot;");
    s = s.replace(/\n/g, "<br>");
    return s;
}
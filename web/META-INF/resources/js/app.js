'use strict';

/* App Module */

var blogApp = angular.module('mudrodApp', [
    'ngRoute',     
    'mudrodControllers',
    'mudrodServices'  
]);


blogApp.config(['$routeProvider', '$locationProvider',
    function($routeProvider, $locationProvider) {
        $routeProvider.
                when('/', {
                    templateUrl: 'partials/search.html'
                    //controller: 'searchCtrl'
                }).when('/metadataView/', {
                    templateUrl: 'partials/metadataResults.html',
                    controller: 'metadataViewCtrl'
                }).when('/metadataView/:name', {
                    templateUrl: 'partials/metadataResults.html'
                    //controller: 'metadataViewCtrl',
                    //controllerAs: 'vm'
                }).when('/datasetView/:shortname', {
                    templateUrl: 'partials/datasetResults.html',
                    controller: 'datasetViewCtrl'
                });

        $locationProvider.html5Mode(false).hashPrefix('!');
    }]);




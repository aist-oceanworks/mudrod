/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you 
 * may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

/* App Module */

var mudrod = angular.module('mudrodApp', [
    'ngRoute',
    'mudrodControllers',
    'mudrodServices'
]);

mudrod.filter('urlencode', function() {
    return function(input) {
        if(input) {
            return window.encodeURIComponent(input);
        }
        return "";
    }
});


mudrod.config(['$routeProvider',
    function ($routeProvider) {
        $routeProvider.when('/', {
            templateUrl: 'partials/search.html'
        }).when('/metadataView/', {
            templateUrl: 'partials/metadataResults.html',
            controller: 'metadataViewCtrl',
            reloadOnSearch: false
        }).when('/datasetView/', {
            templateUrl: 'partials/datasetResults.html',
            controller: 'datasetViewCtrl'
        }).otherwise({
            redirectTo: '/',
            templateUrl: 'partials/search.html'});
    }]);
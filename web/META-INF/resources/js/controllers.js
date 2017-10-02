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

/* Controllers */
var mudrodControllers = angular.module('mudrodControllers', []);

mudrodControllers.controller('searchCtrl', ['$scope', '$rootScope', '$location', '$route', 'Autocomplete', 'SearchOptions',
    function ($scope, $rootScope, $location, $route, Autocomplete, SearchOptions) {

        $scope.hidethis = true;
        $scope.options = {
            opt: 'Or'
        };

        $scope.complete = function (string) {
            Autocomplete.get({term: string},
                function success(response) {
                    $scope.filterSearch = response;
                    $scope.hidethis = false;
                },
                function error(errorResponse) {
                    console.log("Error:" + JSON.stringify(errorResponse));
                }
            );
        };

        $scope.fillTextbox = function (string) {
            $scope.options.query = string;
            $scope.hidethis = true;
            document.getElementById("search-btn").focus();
        };

        $scope.search = function (options) {
            $scope.hidethis = true;
            $rootScope.searchOptions = angular.copy(options);
            $location.path("/metadataView/").search({'query': options.query, 'opt': options.opt});
            $route.reload();
        };

        $scope.$watch(function () {
            return SearchOptions.getSearchOptions();
        }, function (newValue, oldValue) {
            if (newValue !== null) {
                $scope.options.query = newValue.query;
                $scope.options.opt = newValue.opt;
            }
        }, true);
    }]);

mudrodControllers.controller('vocabularyCtrl', ['$scope', '$rootScope', 'VocabList',
    function vocabularyCtrl($scope, $rootScope, VocabList) {

        var word = $rootScope.searchOptions.query;
        VocabList.get({query: word},
            function success(response) {
                $scope.ontologyList = response.graph.ontology;

            },
            function error(errorResponse) {
                console.log("Error:" + JSON.stringify(errorResponse));
            }
        );
    }]);

mudrodControllers.controller('metadataViewCtrl', ['$rootScope', '$scope', '$location', '$routeParams', 'MetaData', 'PagerService', 'SearchOptions',
    function metadataViewCtrl($rootScope, $scope, $location, $routeParams, MetaData, PagerService, SearchOptions) {

        $scope.searchComplete = false;

        var vm = this;
        vm.PDItems = [];
        vm.pager = {};
        vm.setPage = setPage;
        vm.rankData = rankData;
        vm.totalMatches = 0;
        vm.rankopt = $routeParams.rankopt ? $routeParams.rankopt : 'Rank-SVM';

        var word = String();
        var opt = String();
        var rankopt = vm.rankopt;

        if (!$routeParams.query) {
            if(!$rootScope.searchOptions){
                $location.path('/').search({});
            }
            word = $rootScope.searchOptions.query;
            opt = $rootScope.searchOptions.opt;
            SearchOptions.setSearchOptions({'query': word, 'opt': opt});
        } else {
            word = $routeParams.query;
            if($routeParams.opt){
                opt = $routeParams.opt;
            }else if ($rootScope.searchOptions && $rootScope.searchOptions.opt){
                opt = $rootScope.searchOptions.opt;
                upsertQueryParam('opt', opt);
            }else{
                opt = 'Or';
                upsertQueryParam('opt', opt);
            }

            word = word.replace(/,/g , " ");
            word = word.replace(/\//g , " ");

            SearchOptions.setSearchOptions({'query': word, 'opt': opt});
        }
        $rootScope.searchOptions = SearchOptions.getSearchOptions();
        searchMetadata();

        function initController() {
            vm.setPage(1);
        }

        function setPage(page) {
            if (page < 1 || page > vm.pager.totalPages) {
                return;
            }

            // get pager object from service
            vm.pager = PagerService.GetPager(vm.PDItems.length, page);

            // get current page of items
            vm.items = vm.PDItems.slice(vm.pager.startIndex, vm.pager.endIndex + 1);
        }

        function rankData(opt) {
            rankopt = opt;
            upsertQueryParam('rankopt', opt);
            searchMetadata();
        }

        function upsertQueryParam(paramName, paramValue){
            var qParams = $location.search();
            qParams[paramName] = paramValue;
            $location.search(qParams);
        }

        function searchMetadata() {
            MetaData.get({query: word, operator: opt, rankoption: rankopt},
                function success(response) {
                    vm.PDItems = response.PDResults;
                    vm.totalMatches = vm.PDItems.length;
                    vm.query = word;
                    vm.opt = opt;
                    initController();
                    $scope.searchComplete = true;
                },
                function error(errorResponse) {
                    $scope.searchComplete = true;
                    vm.searchError = {"status": errorResponse.status, "message": errorResponse.data};
                }
            );
        }
    }]);

mudrodControllers.controller('datasetViewCtrl', ['$rootScope', '$scope', '$routeParams', 'DatasetDetail', 'SearchOptions',
    function datasetViewCtrl($rootScope, $scope, $routeParams, DatasetDetail, SearchOptions) {
        var shortname = $routeParams.shortname;

        var query = String();
        var opt = String();
        if ($rootScope.searchOptions) {
            query = $rootScope.searchOptions.query;
            opt = $rootScope.searchOptions.opt;
            SearchOptions.setSearchOptions({'query': query, 'opt': opt});
        } else if ($routeParams.query && $routeParams.opt) {
            query = $routeParams.query;
            opt = $routeParams.opt;
            SearchOptions.setSearchOptions({'query': query, 'opt': opt});
            $rootScope.searchOptions = SearchOptions.getSearchOptions();
        }

        DatasetDetail.get({shortname: shortname},
            function success(response) {
                $scope.dataset = response.PDResults[0];
            },
            function error(errorResponse) {
                console.log("Error:" + JSON.stringify(errorResponse));
            }
        );
    }]);

mudrodControllers.controller('hRecommendationCtrl', ['$scope', '$routeParams', 'HRecommendation',
    function hRecommendationCtrl($scope, $routeParams, HRecommendation) {
        var shortname = $routeParams.shortname;
        HRecommendation.get({shortname: shortname},
            function success(response) {
                $scope.recommendationList = response.HybridRecommendationData.linked;

            },
            function error(errorResponse) {
                console.log("Error:" + JSON.stringify(errorResponse));
            }
        );
    }]);




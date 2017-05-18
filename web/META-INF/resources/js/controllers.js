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

mudrodControllers.controller('searchCtrl', ['$scope', '$rootScope', '$location', 'Autocomplete',
    function ($scope, $rootScope, $location, Autocomplete) {
        $scope.options = {
            preference: 'Phrase'
        };
        //$scope.relatedTerms = [];
        //$rootScope.searchOptions = {};
        
        $scope.complete = function(string) {  
            $scope.hidethis = false;  
            var output = [];  
            Autocomplete.get({term: string},
                function success(response) {
                    //alert($scope.challenge.question);
                    //console.log("Success:" + JSON.stringify(response));
                    //$scope.blogList = response;
                    $scope.filterSearch = response;

                },
                function error(errorResponse) {
                    console.log("Error:" + JSON.stringify(errorResponse));
                }
            );  
        }  

        $scope.fillTextbox = function(string){  
            $scope.options.term = string;  
            $scope.hidethis = true;  
        }  

        $scope.search = function(options) {
            $rootScope.searchOptions = angular.copy(options);
            
            $location.path("/metadataView/"+options.term);

        };

    }]);
    
mudrodControllers.controller('vocabularyCtrl', ['$scope', '$rootScope', 'VocabList',
    function vocabularyCtrl($scope, $rootScope, VocabList) {

        var word = $rootScope.searchOptions.term;
        var opt = $rootScope.searchOptions.preference;
        VocabList.get({query: word},
                function success(response) {
                    //alert($scope.challenge.question);
                    //console.log("Success:" + JSON.stringify(response));
                    //$scope.blogList = response;
                    $scope.ontologyList = response.graph.ontology;

                },
                function error(errorResponse) {
                    console.log("Error:" + JSON.stringify(errorResponse));
                }
        );
    }]);

mudrodControllers.controller('metadataViewCtrl', ['$rootScope', '$scope', '$routeParams', 'MetaData', 'PagerService',
    function metadataViewCtrl($rootScope, $scope, $routeParams, MetaData, PagerService) {
        var vm = this;
        vm.PDItems = [];
        vm.pager = {};
        vm.setPage = setPage;
        vm.totalMatches = 0;

        var word = new String();
        var opt = new String();
        
        if(!$routeParams.name){
            word = $rootScope.searchOptions.term;    
            opt = $rootScope.searchOptions.preference; 
        } else {
            word = $routeParams.name;
            var searchKeyWords = new String();

            if (word.search(',') != -1) {
                var topics = word.split(',');
                for (var i = 0; i < topics.length; i++) {
                    searchKeyWords += topics[i];
                }
            } else {
                searchKeyWords = word;
            }
            
            if (searchKeyWords.search('/') != -1 ) {
                var topics = searchKeyWords.split('/');
                searchKeyWords = "";
                for (var i = 0; i < topics.length; i++) {
                    searchKeyWords += topics[i];
                }
            } 
            $rootScope.searchOptions.term = searchKeyWords;
            word = searchKeyWords;
            opt = 'And';
        }

        function initController() {
            // initialize to page 1
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

        this.searchTopic = function(topic) {
            var searchKeyWords = new String();

            if (topic.search(',') != -1 ) {
                var topics = topic.split(',');
                for (var i = 0; i < topics.length; i++) {
                    searchKeyWords += topics[i];
                }
            } else {
                searchKeyWords = topic;
            } 
            
            if (searchKeyWords.search('/') != -1) {
                var topics = searchKeyWords.split('/');
                searchKeyWords = "";
                for (var i = 0; i < topics.length; i++) {
                    searchKeyWords += topics[i];
                }
            } 
            
            $rootScope.searchOptions.term = searchKeyWords;
            MetaData.get({query: searchKeyWords, operator: opt}, 
                function success(response) {
                    vm.PDItems = response.PDResults;
                    vm.totalMatches = vm.PDItems.length;
                    initController();
                    //$scope.PDResults = response.PDResults;

                },
                function error(errorResponse) {
                    console.log("Error:" + JSON.stringify(errorResponse));
                }
            );
        }

        MetaData.get({query: word, operator: opt}, 
                function success(response) {
                    vm.PDItems = response.PDResults;
                    vm.totalMatches = vm.PDItems.length;
                    initController();
                    //$scope.PDResults = response.PDResults;

                },
                function error(errorResponse) {
                    console.log("Error:" + JSON.stringify(errorResponse));
                }
        );
    }]);

mudrodControllers.controller('datasetViewCtrl', ['$scope', '$routeParams', 'DatasetDetail',
    function datasetViewCtrl($scope, $routeParams, DatasetDetail) {
        var shortname = $routeParams.shortname;   

        DatasetDetail.get({shortname: shortname}, 
                function success(response) {
                    //alert($scope.challenge.question);
                    // console.log("Success:" + JSON.stringify(response));
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
                    //alert($scope.challenge.question);
                    // console.log("Success:" + JSON.stringify(response));
                    $scope.recommendationList = response.HybridRecommendationData.linked;

                },
                function error(errorResponse) {
                    console.log("Error:" + JSON.stringify(errorResponse));
                }
        );
    }]);

mudrodControllers.controller('TabCtrl', ['$scope', 
    function($scope) {
    $scope.tab = 1;

    $scope.setTab = function(newTab){
      $scope.tab = newTab;
    };

    $scope.isSet = function(tabNum){
      return $scope.tab === tabNum;
    };
}]);


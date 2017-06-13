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

/* Services */

var mudrodServices = angular.module('mudrodServices', ['ngResource']);

mudrodServices.factory('MetaData', ['$resource',
    function($resource) {
        return $resource("services/metadata/search/", {}, {
            get: {method: 'GET', cache: false, isArray: false}
        });
    }]);

mudrodServices.factory('VocabList', ['$resource',
    function($resource) {
        return $resource("services/vocabulary/search/", {}, {
            get: {method: 'GET', cache: false, isArray: false}            
        });
    }]);

mudrodServices.factory('Autocomplete', ['$resource',
    function($resource) {
        return $resource("services/autocomplete/query", {}, {
            get: {method: 'GET', cache: false, isArray: true}            
        });
    }]);

mudrodServices.factory('DatasetDetail', ['$resource',
    function($resource) {
        return $resource("services/datasetdetail/search/", {}, {
            get: {method: 'GET', cache: false, isArray: false}
        });
    }]);

mudrodServices.factory('HRecommendation', ['$resource',
    function($resource) {
        return $resource("services/hrecommendation/search/", {}, {
            get: {method: 'GET', cache: false, isArray: false}
        });
    }]);

mudrodServices.factory('SearchOptions', function(){
	var searchOptions = {'opt':'Or'};
	return {
	    getSearchOptions: function () {
	        return searchOptions;
	    },
	    setSearchOptions: function (value) {
	    	searchOptions = value;
	    }
	};
});

mudrodServices.factory('PagerService',
    function() {
        // service definition
        var service = {};

        service.GetPager = GetPager;

        return service;

        // service implementation
        function GetPager(totalItems, currentPage, pageSize) {
            // default to first page
            currentPage = currentPage || 1;

            // default page size is 10
            pageSize = pageSize || 10;

            // calculate total pages
            var totalPages = Math.ceil(totalItems / pageSize);

            var startPage, endPage;
            if (totalPages <= 10) {
                // less than 10 total pages so show all
                startPage = 1;
                endPage = totalPages;
            } else {
                // more than 10 total pages so calculate start and end pages
                if (currentPage <= 6) {
                    startPage = 1;
                    endPage = 10;
                } else if (currentPage + 4 >= totalPages) {
                    startPage = totalPages - 9;
                    endPage = totalPages;
                } else {
                    startPage = currentPage - 5;
                    endPage = currentPage + 4;
                }
            }

            // calculate start and end item indexes
            var startIndex = (currentPage - 1) * pageSize;
            var endIndex = Math.min(startIndex + pageSize - 1, totalItems - 1);

            // create an array of pages to ng-repeat in the pager control
            var pages = _.range(startPage, endPage + 1);

            // return object with all pager properties required by the view
            return {
                totalItems: totalItems,
                currentPage: currentPage,
                pageSize: pageSize,
                totalPages: totalPages,
                startPage: startPage,
                endPage: endPage,
                startIndex: startIndex,
                endIndex: endIndex,
                pages: pages
            };
        }
    });
module.exports = require('spc').describe('Other:', function () {
	var schema = require('../schema');

	describe('Saving data with a single quote', function () {
		beforeEach(function () {
			this.subject = new schema.Post({
				title: "Don't"
			});
		});

		it('should save', function (done) {
			this.subject.save(done);
		});
	});
});

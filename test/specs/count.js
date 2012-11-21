module.exports = require('spc').describe('Accessors:', function () {
	var schema = require('../schema');

	xdescribe('count', function () {
			beforeEach(function (end) {
				var that = this;

				User.count(this.where || {}, this.limit, this.offset, function (err, count) {
					if (err) return end(err);

					that.count = count;
					end();
				});
			});

			it('should include all saved rows', function () {
				(saveCount + fixtures.users.length).should.equal(this.count);
			});
		});
});
